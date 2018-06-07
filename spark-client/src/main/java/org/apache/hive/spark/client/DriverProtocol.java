package org.apache.hive.spark.client;

import com.google.common.base.Throwables;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.api.java.JavaFutureAction;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.Future;

class DriverProtocol extends BaseProtocol {

  private final RemoteDriver remoteDriver;

  DriverProtocol(RemoteDriver remoteDriver) {
    this.remoteDriver = remoteDriver;
  }

  Future<Void> sendError(Throwable error) {
    remoteDriver.LOG.debug("Send error to Client: {}", Throwables.getStackTraceAsString(error));
    return remoteDriver.clientRpc.call(new Error(Throwables.getStackTraceAsString(error)));
  }

  Future<Void> sendErrorMessage(String cause) {
    remoteDriver.LOG.debug("Send error to Client: {}", cause);
    return remoteDriver.clientRpc.call(new Error(cause));
  }

  <T extends Serializable> Future<Void> jobFinished(String jobId, T result,
                                            Throwable error, SparkCounters counters) {
    remoteDriver.LOG.debug("Send job({}) result to Client.", jobId);
    return remoteDriver.clientRpc.call(new JobResult(jobId, result, error, counters));
  }

  Future<Void> jobStarted(String jobId) {
    return remoteDriver.clientRpc.call(new JobStarted(jobId));
  }

  Future<Void> jobSubmitted(String jobId, int sparkJobId) {
    remoteDriver.LOG.debug("Send job({}/{}) submitted to Client.", jobId, sparkJobId);
    return remoteDriver.clientRpc.call(new JobSubmitted(jobId, sparkJobId));
  }

  Future<Void> sendMetrics(String jobId, int sparkJobId, int stageId, long taskId, Metrics metrics) {
    remoteDriver.LOG.debug("Send task({}/{}/{}/{}) metric to Client.", jobId, sparkJobId, stageId, taskId);
    return remoteDriver.clientRpc.call(new JobMetrics(jobId, sparkJobId, stageId, taskId, metrics));
  }

  private void handle(ChannelHandlerContext ctx, CancelJob msg) {
    JobWrapper<?> job = remoteDriver.activeJobs.get(msg.id);
    if (job == null || !remoteDriver.cancelJob(job)) {
      remoteDriver.LOG.info("Requested to cancel an already finished client job.");
    }
  }

  private void handle(ChannelHandlerContext ctx, EndSession msg) {
    remoteDriver.LOG.debug("Shutting down due to EndSession request.");
    remoteDriver.shutdown(null);
  }

  private void handle(ChannelHandlerContext ctx, JobRequest msg) {
    remoteDriver.LOG.debug("Received client job request {}", msg.id);
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

  @Override
  public String name() {
    return "Remote Spark Driver to HiveServer2 Connection";
  }
}