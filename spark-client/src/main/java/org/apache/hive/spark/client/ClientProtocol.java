package org.apache.hive.spark.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;


public class ClientProtocol extends BaseProtocol {

  private static final Logger LOG = LoggerFactory.getLogger(ClientProtocol.class);

  private final SparkClient sparkClient;
  private final Map<String, JobHandleImpl<?>> jobs;
  private final Map<String, BlockingQueue> msgResponses;

  public ClientProtocol(SparkClient sparkClient) {
    this.sparkClient = sparkClient;
    this.jobs = Maps.newConcurrentMap();
    this.msgResponses = Maps.newConcurrentMap();
  }

  <T extends Serializable> JobHandleImpl<T> submit(Job<T> job, List<JobHandle.Listener<T>> listeners) {

    final String jobId = UUID.randomUUID().toString();
    final Promise<T> promise = sparkClient.getDriverRpc().createPromise();
    final JobHandleImpl<T> handle = new JobHandleImpl<>(sparkClient, promise, jobId, listeners);
    jobs.put(jobId, handle);

    final io.netty.util.concurrent.Future<Void> rpc = sparkClient.getDriverRpc().call(new JobRequest<>(jobId, job));
    LOG.debug("Send JobRequest[{}].", jobId);

    // Link the RPC and the promise so that events from one are propagated to the other as
    // needed.
    rpc.addListener((GenericFutureListener<Future<Void>>) f -> {
      if (f.isSuccess()) {
        // If the spark job finishes before this listener is called, the QUEUED status will not be set
        handle.changeState(JobHandle.State.QUEUED);
      } else if (!promise.isDone()) {
        promise.setFailure(f.cause());
      }
    });

    promise.addListener((GenericFutureListener<Promise<T>>) p -> {
      jobs.remove(jobId);
      if (p.isCancelled() && !rpc.isDone()) {
        rpc.cancel(true);
      }
    });
    return handle;
  }

  <T extends Serializable> java.util.concurrent.Future<T> run(Job<T> job) {

    @SuppressWarnings("unchecked") final Future<T> rpc = (Future<T>) sparkClient.getDriverRpc().call(new SyncJobRequest(job), Serializable.class);
    return rpc;
  }

  void cancel(String jobId) {
    sparkClient.getDriverRpc().call(new CancelJob(jobId));
  }

  java.util.concurrent.Future<?> endSession() {
    return sparkClient.getDriverRpc().call(new EndSession());
  }

  private void handle(ChannelHandlerContext ctx, Error msg) {
    LOG.warn("Error reported from Remote Spark Driver: {}", msg.cause);
  }

  private void handle(ChannelHandlerContext ctx, JobMetrics msg) {
    JobHandleImpl<?> handle = jobs.get(msg.jobId);
    if (handle != null) {
      handle.getMetrics().addMetrics(msg.sparkJobId, msg.stageId, msg.taskId, msg.metrics);
    } else {
      LOG.warn("Received metrics for unknown Spark job {}", msg.sparkJobId);
    }
  }

  private void handle(ChannelHandlerContext ctx, JobResult msg) {
    JobHandleImpl<?> handle = jobs.remove(msg.id);
    if (handle != null) {
      LOG.debug("Received result for client job {}", msg.id);
      handle.setSparkCounters(msg.sparkCounters);
      Throwable error = msg.error;
      if (error == null) {
        handle.setSuccess(msg.result);
      } else {
        handle.setFailure(error);
      }
    } else {
      LOG.warn("Received result for unknown client job {}", msg.id);
    }
  }

  private void handle(ChannelHandlerContext ctx, JobStarted msg) {
    JobHandleImpl<?> handle = jobs.get(msg.id);
    if (handle != null) {
      handle.changeState(JobHandle.State.STARTED);
    } else {
      LOG.warn("Received event for unknown client job {}", msg.id);
    }
  }

  private void handle(ChannelHandlerContext ctx, JobSubmitted msg) {
    JobHandleImpl<?> handle = jobs.get(msg.clientJobId);
    if (handle != null) {
      LOG.info("Received Spark job ID: {} for client job {}", msg.sparkJobId, msg.clientJobId);
      handle.addSparkJobId(msg.sparkJobId);
    } else {
      LOG.warn("Received Spark job ID: {} for unknown client job {}", msg.sparkJobId, msg.clientJobId);
    }
  }

  // We define the protocol for the RemoteProcessDriver in the same class because the underlying
  // RPC implementation only supports specifying a single RpcDispatcher and it doesn't support
  // polymorphism

  public void startSession(byte[] hiveConfBytes) {
    LOG.debug("Sending startSession request");
    sparkClient.getDriverRpc().call(new StartSession(hiveConfBytes));
  }

  public Exception run(String command, byte[] hiveConfBytes, String queryId) {
    LOG.debug("Sending run command request for query id " + queryId);
    return sendMessage(queryId, new RunCommand(command, hiveConfBytes, queryId));
  }

  public boolean getResults(String queryId, List res) {
    LOG.debug("Sending get results request for query id " + queryId);
    CommandResults commandResults = sendMessage(queryId, new GetResults(queryId));
    res.addAll(commandResults.res);
    return commandResults.moreResults;
  }

  public Exception compileAndRespond(String queryId, String command, byte[] hiveConfBytes) {
    LOG.debug("Sending run command request for query id " + queryId);
    return sendMessage(queryId, new CompileCommand(command, hiveConfBytes, queryId));
  }

  public Exception run(String queryId) {
    LOG.debug("Sending run command request for query id " + queryId);
    return sendMessage(queryId, new RunCommand(null, null, queryId));
  }

  public boolean hasResultSet(String queryId) {
    LOG.debug("Sending hasResultSet request for queryId " + queryId);
    return sendMessage(queryId, new HasResultSet(queryId));
  }

  public byte[] getSchema(String queryId) {
    LOG.debug("Sending getSchema request for queryId " + queryId);
    return sendMessage(queryId, new GetSchema(queryId));
  }

  public boolean isFetchingTable(String queryId) {
    LOG.debug("Sending isFetchingTable request for queryId " + queryId);
    return sendMessage(queryId, new IsFetchingTable(queryId));
  }

  public void closeDriver(String queryId) {
    LOG.debug("Sending closeDriver request for queryId " + queryId);
    sparkClient.getDriverRpc().call(new CloseDriverRequest(queryId));
  }

  public void destroyDriver(String queryId) {
    LOG.debug("Sending destroyDriver request for queryId " + queryId);
    sparkClient.getDriverRpc().call(new DestroyDriverRequest(queryId));
  }

   /**
   * Sends a message to the {@link RemoteDriver} via the Driver
   * {@link org.apache.hive.spark.client.rpc.Rpc} and blocks until the results are received by
   * the corresponding "handle" method.
   */
  private <T> T sendMessage(String queryId, Object msg) {
    BlockingQueue<T> msgResponse = new ArrayBlockingQueue<>(1);
    Preconditions.checkState(msgResponses.putIfAbsent(queryId, msgResponse) == null);
    sparkClient.getDriverRpc().call(msg);
    T response;
    try {
      response = msgResponse.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    msgResponses.remove(queryId);
    return response;
  }

  private void handle(ChannelHandlerContext ctx, CommandResults msg) {
    LOG.debug("Received command results response for query id " + msg.queryId);
    handleMessageResponse(msg.queryId, msg);
  }

  private void handle(ChannelHandlerContext ctx, CommandProcessorResponseMessage msg) {
    LOG.debug("Received command processor response for query id " + msg.queryId);
    handleMessageResponse(msg.queryId, msg.commandProcessorResponse);
  }

  private void handle(ChannelHandlerContext ctx, HasResultSetResponse msg) {
    LOG.debug("Received has result set response for query id " + msg.queryId);
    handleMessageResponse(msg.queryId, msg.hasResultSet);
  }

  private void handle(ChannelHandlerContext ctx, GetSchemaResponse msg) {
    LOG.debug("Received has getSchema response for query id " + msg.queryId);
    handleMessageResponse(msg.queryId, msg.schema);
  }

  private void handle(ChannelHandlerContext ctx, IsFetchingTableResponse msg) {
    LOG.debug("Received has isFetchingTable response for query id " + msg.queryId);
    handleMessageResponse(msg.queryId, msg.isFetchingTableResponse);
  }

  private void handleMessageResponse(String queryId, Object response) {
    Preconditions.checkState(msgResponses.get(queryId).remainingCapacity() == 1);
    msgResponses.get(queryId).add(response);
  }

  @Override
  protected String name() {
    return "HiveServer2 to Remote Spark Driver Connection";
  }
}
