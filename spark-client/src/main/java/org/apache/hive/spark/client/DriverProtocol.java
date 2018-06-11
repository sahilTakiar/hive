package org.apache.hive.spark.client;

import com.google.common.base.Throwables;
import io.netty.channel.ChannelHandlerContext;
import org.apache.hive.spark.client.BaseProtocol;
import org.apache.hive.spark.client.DriverJobWrapper;
import org.apache.hive.spark.client.JobWrapper;
import org.apache.hive.spark.client.MonitorCallback;
import org.apache.hive.spark.client.RemoteDriver;
import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.api.java.JavaFutureAction;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

class DriverProtocol extends BaseProtocol {

  private final RemoteDriver remoteDriver;
  private final QueryExecutorService queryExecutorService;

  DriverProtocol(RemoteDriver remoteDriver) {
    this.remoteDriver = remoteDriver;
    try {
      this.queryExecutorService = (QueryExecutorService) Class.forName("org.apache.hadoop.hive" +
            ".ql.exec.spark.RemoteDriverQueryExecutorService").newInstance();
    } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
      throw new RuntimeException(e);
    }
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

  void sendResults(String[] res) {
    remoteDriver.clientRpc.call(new SendResults(res));
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

  private void handle(ChannelHandlerContext ctx, ExecuteStatement msg) {
    remoteDriver.LOG.debug("Received client execute statement request");
    //DriverJobWrapper<?> wrapper = new DriverJobWrapper<Serializable>(remoteDriver, msg);
//    remoteDriver.activeJobs.put(msg.id, wrapper);
    //remoteDriver.submit(wrapper);
    this.queryExecutorService.run(msg.statement);
  }

  private void handle(ChannelHandlerContext ctx, GetResults msg) {
    remoteDriver.LOG.debug("Received client execute statement request");
    //DriverJobWrapper<?> wrapper = new DriverJobWrapper<Serializable>(remoteDriver, msg);
//    remoteDriver.activeJobs.put(msg.id, wrapper);
    //remoteDriver.submit(wrapper);
    List<String> res = new ArrayList<>();
    try {
      this.queryExecutorService.getResults(res);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    sendResults(res.toArray(new String[res.size()]));
  }

  // TODO hack for now could be to define an interface with run and getResults method inside the
  // spark-client module; then ExecuteStatement would have this interface as a parameter and then
  // implementation inside ql would be to cache the driver, then the driver protocol cause the
  // interface and calls getResults when the request is made

  // TODO would should be the long term fix for this? basically need a way to launch a Driver in
  // the spark-client library using classes from the ql library - the above solution actually
  // makes a lot of sense

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