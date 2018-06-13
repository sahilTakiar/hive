package org.apache.hive.spark.client;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.api.java.JavaFutureAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

class DriverProtocol extends BaseProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(DriverProtocol.class);

  private final RemoteDriver remoteDriver;
  private final RemoteProcessDriverExecutorFactory remoteProcessDriverExecutorFactory;
  private final Map<String, RemoteProcessDriverExecutor> commands = Maps.newConcurrentMap();

  DriverProtocol(RemoteDriver remoteDriver) {
    this.remoteDriver = remoteDriver;
    this.remoteProcessDriverExecutorFactory = createRemoteProcessDriverExecutorFactory();
  }

  Future<Void> sendError(Throwable error) {
    LOG.debug("Send error to Client: {}", Throwables.getStackTraceAsString(error));
    return remoteDriver.clientRpc.call(new Error(Throwables.getStackTraceAsString(error)));
  }

  Future<Void> sendErrorMessage(String cause) {
    LOG.debug("Send error to Client: {}", cause);
    return remoteDriver.clientRpc.call(new Error(cause));
  }

  <T extends Serializable> Future<Void> jobFinished(String jobId, T result,
                                            Throwable error, SparkCounters counters) {
    LOG.debug("Send job({}) result to Client.", jobId);
    return remoteDriver.clientRpc.call(new JobResult(jobId, result, error, counters));
  }

  Future<Void> jobStarted(String jobId) {
    return remoteDriver.clientRpc.call(new JobStarted(jobId));
  }

  Future<Void> jobSubmitted(String jobId, int sparkJobId) {
    LOG.debug("Send job({}/{}) submitted to Client.", jobId, sparkJobId);
    return remoteDriver.clientRpc.call(new JobSubmitted(jobId, sparkJobId));
  }

  Future<Void> sendMetrics(String jobId, int sparkJobId, int stageId, long taskId, Metrics metrics) {
    LOG.debug("Send task({}/{}/{}/{}) metric to Client.", jobId, sparkJobId, stageId, taskId);
    return remoteDriver.clientRpc.call(new JobMetrics(jobId, sparkJobId, stageId, taskId, metrics));
  }

  private void handle(ChannelHandlerContext ctx, CancelJob msg) {
    JobWrapper<?> job = remoteDriver.activeJobs.get(msg.id);
    if (job == null || !remoteDriver.cancelJob(job)) {
      LOG.info("Requested to cancel an already finished client job.");
    }
  }

  private void handle(ChannelHandlerContext ctx, EndSession msg) {
    LOG.debug("Shutting down due to EndSession request.");
    remoteDriver.shutdown(null);
  }

  private void handle(ChannelHandlerContext ctx, JobRequest msg) {
    LOG.debug("Received client job request {}", msg.id);
    JobWrapper<?> wrapper = new JobWrapper<Serializable>(remoteDriver, msg);
    remoteDriver.activeJobs.put(msg.id, wrapper);
    remoteDriver.submit(wrapper);
  }

  private Object handle(ChannelHandlerContext ctx, SyncJobRequest msg) throws Exception {
    // In case the job context is not up yet, let's wait, since this is supposed to be a
    // "synchronous" RPC.
    if (remoteDriver.jc == null) {
      synchronized (remoteDriver.jcLock) {
        while (remoteDriver.jc == null) {
          remoteDriver.jcLock.wait();
          if (!remoteDriver.running) {
            throw new IllegalStateException("Remote Spark context is shutting down.");
          }
        }
      }
    }

    remoteDriver.jc.setMonitorCb(new MonitorCallback() {
      @Override
      public void call(JavaFutureAction<?> future,
          SparkCounters sparkCounters, Set<Integer> cachedRDDIds) {
        throw new IllegalStateException(
          "JobContext.monitor() is not available for synchronous jobs.");
      }
    });
    try {
      return msg.job.call(remoteDriver.jc);
    } finally {
      remoteDriver.jc.setMonitorCb(null);
    }
  }

  // We define the protocol for the RemoteProcessDriver in the same class because the underlying
  // RPC implementation only supports specifying a single RpcDispatcher and it doesn't support
  // polymorphism

  private void handle(ChannelHandlerContext ctx, RunCommand msg) {
    LOG.debug("Received client run command request for query id " + msg.queryId);

    // TODO fix this, this is suppose to be run in a threadpool, right now submit is just
    // directly invoked
    if (msg.command != null) {
      remoteDriver.submit(() -> {
        RemoteProcessDriverExecutor remoteProcessDriverExecutor = remoteProcessDriverExecutorFactory.createRemoteProcessDriverExecutor(
                msg.command, msg.hiveConfBytes, msg.queryId);
        commands.put(msg.queryId, remoteProcessDriverExecutor);
        Exception commandProcessorResponse = remoteProcessDriverExecutor.run(msg.command);
        remoteDriver.clientRpc.call(new CommandProcessorResponseMessage(msg
                .queryId, commandProcessorResponse));
      });
    } else {
      remoteDriver.submit(() -> {
        commands.get(msg.queryId).run();
      });
    }
  }

  private void handle(ChannelHandlerContext ctx, CompileCommand msg) {
    LOG.debug("Received client get results request");

   remoteDriver.submit(() -> {
      RemoteProcessDriverExecutor remoteProcessDriverExecutor = remoteProcessDriverExecutorFactory.createRemoteProcessDriverExecutor(
              msg.command, msg.hiveConfBytes, msg.queryId);
      commands.put(msg.queryId, remoteProcessDriverExecutor);
      Exception commandProcessorResponse = remoteProcessDriverExecutor.compileAndRespond(msg
              .command);
    });
  }

  private void handle(ChannelHandlerContext ctx, GetResults msg) {
    LOG.debug("Received client get results request");

    remoteDriver.submit(() -> {
      List res = new ArrayList();
      try {
        boolean moreResults = commands.get(msg.queryId).getResults(res);
        remoteDriver.clientRpc.call(new CommandResults(res, msg.queryId, moreResults));
      } catch (IOException e) {
        // TODO how are exceptions handled?
        throw new RuntimeException(e);
      }
    });
  }

  private RemoteProcessDriverExecutorFactory createRemoteProcessDriverExecutorFactory() {
    try {
      return (RemoteProcessDriverExecutorFactory) Class.forName("org.apache.hadoop.hive.ql.exec" +
              ".spark.RemoteProcessDriverExecutorFactoryImpl").newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String name() {
    return "Remote Spark Driver to HiveServer2 Connection";
  }
}