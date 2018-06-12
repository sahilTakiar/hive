/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import com.google.common.io.Files;
import io.netty.channel.nio.NioEventLoopGroup;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.client.rpc.Rpc;
import org.apache.hive.spark.client.rpc.RpcConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Driver code for the Spark client library.
 */
@InterfaceAudience.Private
public class RemoteDriver {

  public static final Logger LOG = LoggerFactory.getLogger(RemoteDriver.class);
  private static RemoteDriver instance;

  public final Map<String, JobWrapper<?>> activeJobs;
  public final Object jcLock;
  private final Object shutdownLock;
  public final ExecutorService executor;
  private final NioEventLoopGroup egroup;
  public final Rpc clientRpc;
  final DriverProtocol protocol;
  // a local temp dir specific to this driver
  private final File localTmpDir;

  // Used to queue up requests while the SparkContext is being created.
  final List<Submittable> jobQueue = Lists.newLinkedList();

  // jc is effectively final, but it has to be volatile since it's accessed by different
  // threads while the constructor is running.
  public volatile JobContextImpl jc;
  volatile boolean running;

  public static final String REMOTE_DRIVER_HOST_CONF = "--remote-host";
  public static final String REMOTE_DRIVER_PORT_CONF = "--remote-port";
  public static final String REMOTE_DRIVER_CONF = "--remote-driver-conf";

  private final long futureTimeout; // Rpc call timeout in milliseconds

  private RemoteDriver(String[] args) throws Exception {
    this.activeJobs = Maps.newConcurrentMap();
    this.jcLock = new Object();
    this.shutdownLock = new Object();
    localTmpDir = Files.createTempDir();

    addShutdownHook();

    SparkConf conf = new SparkConf();
    String serverAddress = null;
    int serverPort = -1;
    Map<String, String> mapConf = Maps.newHashMap();
    for (int idx = 0; idx < args.length; idx += 2) {
      String key = args[idx];
      if (REMOTE_DRIVER_HOST_CONF.equals(key)) {
        serverAddress = getArg(args, idx);
      } else if (REMOTE_DRIVER_PORT_CONF.equals(key)) {
        serverPort = Integer.parseInt(getArg(args, idx));
      } else if (REMOTE_DRIVER_CONF.equals(key)) {
        String[] val = getArg(args, idx).split("[=]", 2);
        //set these only in mapConf and not in SparkConf,
        // as these are non-spark specific configs used by the remote driver
        mapConf.put(val[0], val[1]);
      } else {
        throw new IllegalArgumentException("Invalid command line arguments: "
          + Joiner.on(" ").join(args));
      }
    }

    executor = Executors.newCachedThreadPool();

    LOG.info("Connecting to HiveServer2 address: {}:{}", serverAddress, serverPort);

    for (Tuple2<String, String> e : conf.getAll()) {
      mapConf.put(e._1(), e._2());
      LOG.debug("Remote Spark Driver configured with: " + e._1() + "=" + e._2());
    }

    String clientId = mapConf.get(SparkClientFactory.CONF_CLIENT_ID);
    Preconditions.checkArgument(clientId != null, "No client ID provided.");
    String secret = mapConf.get(SparkClientFactory.CONF_KEY_SECRET);
    Preconditions.checkArgument(secret != null, "No secret provided.");

    RpcConfiguration rpcConf = new RpcConfiguration(mapConf);
    futureTimeout = rpcConf.getFutureTimeoutMs();
    int threadCount = rpcConf.getRpcThreadCount();
    this.egroup = new NioEventLoopGroup(
        threadCount,
        new ThreadFactoryBuilder()
            .setNameFormat("Spark-Driver-RPC-Handler-%d")
            .setDaemon(true)
            .build());
    this.protocol = DriverProtocolFactory.getDriverProtocol(mapConf, this);

    // The RPC library takes care of timing out this.
    this.clientRpc = Rpc.createClient(mapConf, egroup, serverAddress, serverPort,
      clientId, secret, protocol).get();
    this.running = true;

    this.clientRpc.addListener(new Rpc.Listener() {
      @Override
      public void rpcClosed(Rpc rpc) {
        LOG.warn("Shutting down driver because Remote Spark Driver to HiveServer2 connection was closed.");
        shutdown(null);
      }

      @Override
      public String toString() {
        return "Shutting Down Remote Spark Driver to HiveServer2 Connection";
      }
    });

    try {
      JavaSparkContext sc = new JavaSparkContext(conf);
      sc.sc().addSparkListener(new ClientListener());
      synchronized (jcLock) {
        jc = new JobContextImpl(sc, localTmpDir);
        jcLock.notifyAll();
      }
    } catch (Exception e) {
      LOG.error("Failed to start SparkContext: " + e, e);
      shutdown(e);
      synchronized (jcLock) {
        jcLock.notifyAll();
      }
      throw e;
    }

    synchronized (jcLock) {
      for (Iterator<Submittable> it = jobQueue.iterator(); it.hasNext();) {
        it.next().submit();
      }
    }
  }

  public static RemoteDriver getInstance() {
    return instance;
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      if (running) {
        LOG.info("Received signal SIGTERM, attempting safe shutdown of Remote Spark Context");
        protocol.sendErrorMessage("Remote Spark Context was shutdown because it received a SIGTERM " +
                "signal. Most likely due to a kill request via YARN.");
        shutdown(null);
      }
    }));
  }

  private void run() throws InterruptedException {
    synchronized (shutdownLock) {
      while (running) {
        shutdownLock.wait();
      }
    }
    executor.shutdownNow();
    try {
      FileUtils.deleteDirectory(localTmpDir);
    } catch (IOException e) {
      LOG.warn("Failed to delete local tmp dir: " + localTmpDir, e);
    }
  }

  public void submit(JobWrapper<?> job) {
    synchronized (jcLock) {
      if (jc != null) {
        job.submit();
      } else {
        LOG.info("SparkContext not yet up; adding Hive on Spark job request to the queue.");
        jobQueue.add(job);
      }
    }
  }

  synchronized void shutdown(Throwable error) {
    if (running) {
      if (error == null) {
        LOG.info("Shutting down Spark Remote Driver.");
      } else {
        LOG.error("Shutting down Spark Remote Driver due to error: " + error, error);
      }
      running = false;
      for (JobWrapper<?> job : activeJobs.values()) {
        cancelJob(job);
      }

      if (error != null) {
        try {
          protocol.sendError(error).get(futureTimeout, TimeUnit.MILLISECONDS);
        } catch(InterruptedException|ExecutionException |TimeoutException e) {
          LOG.warn("Failed to send out the error during RemoteDriver shutdown", e);
        }
      }
      if (jc != null) {
        jc.stop();
      }
      clientRpc.close();

      egroup.shutdownGracefully();
      synchronized (shutdownLock) {
        shutdownLock.notifyAll();
      }
    }
  }

  boolean cancelJob(JobWrapper<?> job) {
    boolean cancelled = false;
    for (JavaFutureAction<?> action : job.jobs) {
      cancelled |= action.cancel(true);
    }
    return cancelled | (job.future != null && job.future.cancel(true));
  }

  private String getArg(String[] args, int keyIdx) {
    int valIdx = keyIdx + 1;
    if (args.length <= valIdx) {
      throw new IllegalArgumentException("Invalid command line arguments: "
        + Joiner.on(" ").join(args));
    }
    return args[valIdx];
  }

  public JavaSparkContext sc() {
    return this.jc.sc();
  }

  private class ClientListener extends SparkListener {

    private final Map<Integer, Integer> stageToJobId = Maps.newHashMap();

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
      synchronized (stageToJobId) {
        for (int i = 0; i < jobStart.stageIds().length(); i++) {
          stageToJobId.put((Integer) jobStart.stageIds().apply(i), jobStart.jobId());
        }
      }
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd jobEnd) {
      synchronized (stageToJobId) {
        for (Iterator<Map.Entry<Integer, Integer>> it = stageToJobId.entrySet().iterator();
            it.hasNext();) {
          Map.Entry<Integer, Integer> e = it.next();
          if (e.getValue() == jobEnd.jobId()) {
            it.remove();
          }
        }
      }

      String clientId = getClientId(jobEnd.jobId());
      if (clientId != null) {
        activeJobs.get(clientId).jobDone();
      }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
      if (taskEnd.reason() instanceof org.apache.spark.Success$
          && !taskEnd.taskInfo().speculative()) {
        Metrics metrics = new Metrics(taskEnd.taskMetrics(), taskEnd.taskInfo());
        Integer jobId;
        synchronized (stageToJobId) {
          jobId = stageToJobId.get(taskEnd.stageId());
        }

        // TODO: implement implicit AsyncRDDActions conversion instead of jc.monitor()?
        // TODO: how to handle stage failures?

        String clientId = getClientId(jobId);
        if (clientId != null) {
          protocol.sendMetrics(clientId, jobId, taskEnd.stageId(),
            taskEnd.taskInfo().taskId(), metrics);
        }
      }
    }

    /**
     * Returns the client job ID for the given Spark job ID.
     *
     * This will only work for jobs monitored via JobContext#monitor(). Other jobs won't be
     * matched, and this method will return `None`.
     */
    private String getClientId(Integer jobId) {
      for (Map.Entry<String, JobWrapper<?>> e : activeJobs.entrySet()) {
        for (JavaFutureAction<?> future : e.getValue().jobs) {
          if (future.jobIds().contains(jobId)) {
            return e.getKey();
          }
        }
      }
      return null;
    }

  }

  public static void main(String[] args) throws Exception {
    RemoteDriver rd = new RemoteDriver(args);
    RemoteDriver.instance = rd;
    try {
      rd.run();
    } catch (Exception e) {
      // If the main thread throws an exception for some reason, propagate the exception to the
      // client and initiate a safe shutdown
      if (rd.running) {
        rd.shutdown(e);
      }
      throw e;
    }
  }
}

