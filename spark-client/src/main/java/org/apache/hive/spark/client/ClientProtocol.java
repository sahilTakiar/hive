package org.apache.hive.spark.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ClientProtocol extends BaseProtocol {

  private AbstractSparkClient sparkClient;
  private volatile String[] results;
  public Object resultsLock = new Object();

  public ClientProtocol(AbstractSparkClient sparkClient) {
    this.sparkClient = sparkClient;
  }

  public void runStatement(String statement) {
    sparkClient.driverRpc.call(new ExecuteStatement(statement));
  }

  public void sendGetResulsts() {
    sparkClient.driverRpc.call(new GetResults());
  }

  <T extends Serializable> JobHandleImpl<T> submit(Job<T> job, List<JobHandle.Listener<T>> listeners) {
    final String jobId = UUID.randomUUID().toString();
    final Promise<T> promise = sparkClient.driverRpc.createPromise();
    final JobHandleImpl<T> handle =
        new JobHandleImpl<T>(sparkClient, promise, jobId, listeners);
    sparkClient.jobs.put(jobId, handle);

    final io.netty.util.concurrent.Future<Void> rpc = sparkClient.driverRpc.call(new JobRequest(jobId, job));
    AbstractSparkClient.LOG.debug("Send JobRequest[{}].", jobId);

    // Link the RPC and the promise so that events from one are propagated to the other as
    // needed.
    rpc.addListener(new GenericFutureListener<Future<Void>>() {
      @Override
      public void operationComplete(Future<Void> f) {
        if (f.isSuccess()) {
          // If the spark job finishes before this listener is called, the QUEUED status will not be set
          handle.changeState(JobHandle.State.QUEUED);
        } else if (!promise.isDone()) {
          promise.setFailure(f.cause());
        }
      }
    });
    promise.addListener(new GenericFutureListener<Promise<T>>() {
      @Override
      public void operationComplete(Promise<T> p) {
        if (jobId != null) {
          sparkClient.jobs.remove(jobId);
        }
        if (p.isCancelled() && !rpc.isDone()) {
          rpc.cancel(true);
        }
      }
    });
    return handle;
  }

  <T extends Serializable> java.util.concurrent.Future<T> run(Job<T> job) {
    @SuppressWarnings("unchecked")
    final Future<T> rpc = (Future<T>)
      sparkClient.driverRpc.call(new SyncJobRequest(job), Serializable.class);
    return rpc;
  }

  void cancel(String jobId) {
    sparkClient.driverRpc.call(new CancelJob(jobId));
  }

  java.util.concurrent.Future<?> endSession() {
    return sparkClient.driverRpc.call(new EndSession());
  }

  private void handle(ChannelHandlerContext ctx, Error msg) {
    AbstractSparkClient.LOG.warn("Error reported from Remote Spark Driver: {}", msg.cause);
  }

  private void handle(ChannelHandlerContext ctx, JobMetrics msg) {
    JobHandleImpl<?> handle = sparkClient.jobs.get(msg.jobId);
    if (handle != null) {
      handle.getMetrics().addMetrics(msg.sparkJobId, msg.stageId, msg.taskId, msg.metrics);
    } else {
      AbstractSparkClient.LOG.warn("Received metrics for unknown Spark job {}", msg.sparkJobId);
    }
  }

  private void handle(ChannelHandlerContext ctx, JobResult msg) {
    JobHandleImpl<?> handle = sparkClient.jobs.remove(msg.id);
    if (handle != null) {
      AbstractSparkClient.LOG.debug("Received result for client job {}", msg.id);
      handle.setSparkCounters(msg.sparkCounters);
      Throwable error = msg.error;
      if (error == null) {
        handle.setSuccess(msg.result);
      } else {
        handle.setFailure(error);
      }
    } else {
      AbstractSparkClient.LOG.warn("Received result for unknown client job {}", msg.id);
    }
  }

  private void handle(ChannelHandlerContext ctx, JobStarted msg) {
    JobHandleImpl<?> handle = sparkClient.jobs.get(msg.id);
    if (handle != null) {
      handle.changeState(JobHandle.State.STARTED);
    } else {
      AbstractSparkClient.LOG.warn("Received event for unknown client job {}", msg.id);
    }
  }

  private void handle(ChannelHandlerContext ctx, JobSubmitted msg) {
    JobHandleImpl<?> handle = sparkClient.jobs.get(msg.clientJobId);
    if (handle != null) {
      AbstractSparkClient.LOG.info("Received Spark job ID: {} for client job {}", msg.sparkJobId, msg.clientJobId);
      handle.addSparkJobId(msg.sparkJobId);
    } else {
      AbstractSparkClient.LOG.warn("Received Spark job ID: {} for unknown client job {}", msg.sparkJobId, msg.clientJobId);
    }
  }

  private void handle(ChannelHandlerContext ctx, SendResults msg) {
    this.results = msg.res;
  }

  private void handle(ChannelHandlerContext ctx, QueryResults msg) {
    this.results = msg.results;
    this.resultsLock.notify();
  }

  public boolean getResults(List<String> res) {
    res.addAll(Arrays.asList(results));
    return true;
  }

  @Override
  protected String name() {
    return "HiveServer2 to Remote Spark Driver Connection";
  }
}
