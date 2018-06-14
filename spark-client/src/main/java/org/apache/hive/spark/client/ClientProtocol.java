package org.apache.hive.spark.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;

import org.apache.hive.spark.client.rpc.Rpc;
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
  private final Map<String, BlockingQueue<CommandResults>> commandResults;
  private final Map<String, BlockingQueue<Boolean>> commandHasResultSet;
  private final Map<String, BlockingQueue<Boolean>> isFetchingTableRes;
  private final Map<String, BlockingQueue<byte[]>> commandSchema;
  private final Map<String, BlockingQueue<Exception>> commandProcessResponses;
  private final Map<String, BlockingQueue> oneMapToRuleThemAll;

  public ClientProtocol(SparkClient sparkClient) {
    this.sparkClient = sparkClient;
    this.jobs = Maps.newConcurrentMap();
    this.commandResults = Maps.newConcurrentMap();
    this.commandHasResultSet = Maps.newConcurrentMap();
    this.commandSchema = Maps.newConcurrentMap();
    this.isFetchingTableRes = Maps.newConcurrentMap();
    this.commandProcessResponses = Maps.newConcurrentMap();
    this.oneMapToRuleThemAll = Maps.newConcurrentMap();
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

  public Exception run(String command, byte[] hiveConfBytes, String queryId) {
    LOG.debug("Sending run command request for query id " + queryId);
    BlockingQueue<Exception> results = new ArrayBlockingQueue<>(1);
    Preconditions.checkState(oneMapToRuleThemAll.putIfAbsent(queryId, results) == null);
    sparkClient.getDriverRpc().call(new RunCommand(command, hiveConfBytes, queryId));
    Exception response;
    try {
      response = results.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    oneMapToRuleThemAll.remove(queryId);
    return response;
  }

  public boolean getResults(String queryId, List res) {
    LOG.debug("Sending get results request for query id " + queryId);
    BlockingQueue<CommandResults> results = new ArrayBlockingQueue<>(1);
    commandResults.put(queryId, results);
    sparkClient.getDriverRpc().call(new GetResults(queryId));
    CommandResults commandResults1 = null;
    try {
      commandResults1 = results.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    res.addAll(commandResults1.res);
    commandResults.remove(queryId);
    return commandResults1.moreResults;
    // TODO model this as a map of Futures - or something similar, maybe need a request id?
    // could just use a map with key = query id and value = pair(lock, commandresults) - could
    // use striped instead if there are too many locks
    // should it be a single hashmap for all objects, or separate maps for each getRequest?
    // the issue with a single map is that the keys will conflict, which makes the client
    // non-thread safe
  }

  public Exception compileAndRespond(String queryId, String command, byte[] hiveConfBytes) {
    LOG.debug("Sending run command request for query id " + queryId);
    BlockingQueue<Exception> results = new ArrayBlockingQueue<>(1);
    oneMapToRuleThemAll.put(queryId, results);
    sparkClient.getDriverRpc().call(new CompileCommand(command, hiveConfBytes, queryId));
    Exception response;
    try {
      response = results.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    oneMapToRuleThemAll.remove(queryId);
    return response;
  }

  public Exception run(String queryId) {
    LOG.debug("Sending run command request for query id " + queryId);
    BlockingQueue<Exception> results = new ArrayBlockingQueue<>(1);
    oneMapToRuleThemAll.put(queryId, results);
    sparkClient.getDriverRpc().call(new RunCommand(null, null, queryId));
    Exception response;
    try {
      response = results.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    oneMapToRuleThemAll.remove(queryId);
    return response;
  }

  public boolean hasResultSet(String queryId) {
    LOG.debug("Sending hasResultSet request for queryId " + queryId);
    BlockingQueue<Boolean> results = new ArrayBlockingQueue<>(1);
    commandHasResultSet.put(queryId, results);
    sparkClient.getDriverRpc().call(new HasResultSet(queryId));
    boolean response;
    try {
      response = results.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    commandHasResultSet.remove(queryId);
    return response;
  }

  public byte[] getSchema(String queryId) {
    BlockingQueue<byte[]> results = new ArrayBlockingQueue<>(1);
    commandSchema.put(queryId, results);
    sparkClient.getDriverRpc().call(new GetSchema(queryId));
    byte[] response;
    try {
      response = results.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    commandSchema.remove(queryId);
    return response;
  }

  public boolean isFetchingTable(String queryId) {
    LOG.debug("Sending isFetchingTable request for queryId " + queryId);
    BlockingQueue<Boolean> results = new ArrayBlockingQueue<>(1);
    isFetchingTableRes.put(queryId, results);
    sparkClient.getDriverRpc().call(new IsFetchingTable(queryId));
    boolean response;
    try {
      response = results.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    isFetchingTableRes.remove(queryId);
    return response;
  }

  private void handle(ChannelHandlerContext ctx, CommandResults msg) {
    LOG.debug("Received command results for query id " + msg.queryId);
    BlockingQueue<CommandResults> queue = commandResults.get(msg.queryId);
    Preconditions.checkState(commandResults.get(msg.queryId).remainingCapacity() == 1);
    queue.add(msg);
  }

  private void handle(ChannelHandlerContext ctx, CommandProcessorResponseMessage msg) {
    LOG.debug("Received command results for query id " + msg.queryId);
    LOG.debug("Received CommandProcessorResponse " + msg.commandProcessorResponse);
    oneMapToRuleThemAll.get(msg.queryId).add(msg.commandProcessorResponse);
    //commandProcessResponses.get(msg.queryId).add(msg.commandProcessorResponse);
//    BlockingQueue<CommandResults> queue = commandResults.get(msg.queryId);
//    Preconditions.checkState(commandResults.get(msg.queryId).remainingCapacity() == 1);
//    queue.add(msg);
  }

  private void handle(ChannelHandlerContext ctx, HasResultSetResponse msg) {
    commandHasResultSet.get(msg.queryId).add(msg.hasResultSet);
  }

  private void handle(ChannelHandlerContext ctx, GetSchemaResponse msg) {
    commandSchema.get(msg.queryId).add(msg.schema);
  }

  private void handle(ChannelHandlerContext ctx, IsFetchingTableResponse msg) {
    isFetchingTableRes.get(msg.queryId).add(msg.isFetchingTableResponse);
  }

  @Override
  protected String name() {
    return "HiveServer2 to Remote Spark Driver Connection";
  }
}
