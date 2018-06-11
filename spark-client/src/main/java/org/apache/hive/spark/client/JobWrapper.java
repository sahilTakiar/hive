package org.apache.hive.spark.client;

import com.google.common.collect.Lists;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.SparkJobInfo;
import org.apache.spark.api.java.JavaFutureAction;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class JobWrapper<T extends Serializable> implements Callable<Void>, Submittable {

  private RemoteDriver remoteDriver;
  private final BaseProtocol.JobRequest<T> req;
  final List<JavaFutureAction<?>> jobs;
  private final AtomicInteger jobEndReceived;
  private int completed;
  private SparkCounters sparkCounters;
  private Set<Integer> cachedRDDIds;
  private Integer sparkJobId;

  Future<?> future;

  public JobWrapper(RemoteDriver remoteDriver, BaseProtocol.JobRequest<T> req) {
    this.remoteDriver = remoteDriver;
    this.req = req;
    this.jobs = Lists.newArrayList();
    completed = 0;
    jobEndReceived = new AtomicInteger(0);
    this.sparkCounters = null;
    this.cachedRDDIds = null;
    this.sparkJobId = null;
  }

  @Override
  public Void call() throws Exception {
    remoteDriver.protocol.jobStarted(req.id);

    try {
      remoteDriver.jc.setMonitorCb(new MonitorCallback() {
        @Override
        public void call(JavaFutureAction<?> future,
            SparkCounters sparkCounters, Set<Integer> cachedRDDIds) {
          monitorJob(future, sparkCounters, cachedRDDIds);
        }
      });

      T result = req.job.call(remoteDriver.jc);
      // In case the job is empty, there won't be JobStart/JobEnd events. The only way
      // to know if the job has finished is to check the futures here ourselves.
      for (JavaFutureAction<?> future : jobs) {
        future.get();
        completed++;
        RemoteDriver.LOG.debug("Client job {}: {} of {} Spark jobs finished.",
            req.id, completed, jobs.size());
      }

      // If the job is not empty (but runs fast), we have to wait until all the TaskEnd/JobEnd
      // events are processed. Otherwise, task metrics may get lost. See HIVE-13525.
      if (sparkJobId != null) {
        SparkJobInfo sparkJobInfo = remoteDriver.jc.sc().statusTracker().getJobInfo(sparkJobId);
        if (sparkJobInfo != null && sparkJobInfo.stageIds() != null &&
            sparkJobInfo.stageIds().length > 0) {
          synchronized (jobEndReceived) {
            while (jobEndReceived.get() < jobs.size()) {
              jobEndReceived.wait();
            }
          }
        }
      }

      SparkCounters counters = null;
      if (sparkCounters != null) {
        counters = sparkCounters.snapshot();
      }

      remoteDriver.protocol.jobFinished(req.id, result, null, counters);
    } catch (Throwable t) {
      // Catch throwables in a best-effort to report job status back to the client. It's
      // re-thrown so that the executor can destroy the affected thread (or the JVM can
      // die or whatever would happen if the throwable bubbled up).
      RemoteDriver.LOG.error("Failed to run client job " + req.id, t);
      remoteDriver.protocol.jobFinished(req.id, null, t,
          sparkCounters != null ? sparkCounters.snapshot() : null);
      throw new ExecutionException(t);
    } finally {
      remoteDriver.jc.setMonitorCb(null);
      remoteDriver.activeJobs.remove(req.id);
      releaseCache();
    }
    return null;
  }

  @Override
  public void submit() {
    this.future = remoteDriver.executor.submit(this);
  }

  void jobDone() {
    synchronized (jobEndReceived) {
      jobEndReceived.incrementAndGet();
      jobEndReceived.notifyAll();
    }
  }

  /**
   * Release cached RDDs as soon as the job is done.
   * This is different from local Spark client so as
   * to save a RPC call/trip, avoid passing cached RDD
   * id information around. Otherwise, we can follow
   * the local Spark client way to be consistent.
   */
  void releaseCache() {
    if (cachedRDDIds != null) {
      for (Integer cachedRDDId: cachedRDDIds) {
        remoteDriver.jc.sc().sc().unpersistRDD(cachedRDDId, false);
      }
    }
  }

  private void monitorJob(JavaFutureAction<?> job,
      SparkCounters sparkCounters, Set<Integer> cachedRDDIds) {
    jobs.add(job);
    if (!remoteDriver.jc.getMonitoredJobs().containsKey(req.id)) {
      remoteDriver.jc.getMonitoredJobs().put(req.id, new CopyOnWriteArrayList<JavaFutureAction<?>>());
    }
    remoteDriver.jc.getMonitoredJobs().get(req.id).add(job);
    this.sparkCounters = sparkCounters;
    this.cachedRDDIds = cachedRDDIds;
    sparkJobId = job.jobIds().get(0);
    remoteDriver.protocol.jobSubmitted(req.id, sparkJobId);
  }

}
