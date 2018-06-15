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

import java.io.Serializable;
import java.util.List;

import org.apache.hive.spark.client.metrics.Metrics;
import org.apache.hive.spark.client.rpc.RpcDispatcher;
import org.apache.hive.spark.counter.SparkCounters;


public abstract class BaseProtocol extends RpcDispatcher {

  protected static class CancelJob implements Serializable {

    final String id;

    CancelJob(String id) {
      this.id = id;
    }

    CancelJob() {
      this(null);
    }

    @Override
    public String toString() {
      return "CancelJob{" +
              "id='" + id + '\'' +
              '}';
    }
  }

  protected static class EndSession implements Serializable {

    @Override
    public String toString() {
      return "EndSession";
    }
  }

  protected static class Error implements Serializable {

    final String cause;

    Error(String cause) {
      this.cause = cause;
    }

    Error() {
      this(null);
    }

    @Override
    public String toString() {
      return "Error{" +
              "cause='" + cause + '\'' +
              '}';
    }
  }

  protected static class JobMetrics implements Serializable {

    final String jobId;
    final int sparkJobId;
    final int stageId;
    final long taskId;
    final Metrics metrics;

    JobMetrics(String jobId, int sparkJobId, int stageId, long taskId, Metrics metrics) {
      this.jobId = jobId;
      this.sparkJobId = sparkJobId;
      this.stageId = stageId;
      this.taskId = taskId;
      this.metrics = metrics;
    }

    JobMetrics() {
      this(null, -1, -1, -1, null);
    }

    @Override
    public String toString() {
      return "JobMetrics{" +
              "jobId='" + jobId + '\'' +
              ", sparkJobId=" + sparkJobId +
              ", stageId=" + stageId +
              ", taskId=" + taskId +
              ", metrics=" + metrics +
              '}';
    }
  }

  public static class JobRequest<T extends Serializable> implements Serializable {

    final String id;
    final Job<T> job;

    JobRequest(String id, Job<T> job) {
      this.id = id;
      this.job = job;
    }

    JobRequest() {
      this(null, null);
    }

    @Override
    public String toString() {
      return "JobRequest{" +
              "id='" + id + '\'' +
              ", job=" + job +
              '}';
    }
  }

  public static class JobResult<T extends Serializable> implements Serializable {

    final String id;
    final T result;
    final Throwable error;
    final SparkCounters sparkCounters;

    JobResult(String id, T result, Throwable error, SparkCounters sparkCounters) {
      this.id = id;
      this.result = result;
      this.error = error;
      this.sparkCounters = sparkCounters;
    }

    JobResult() {
      this(null, null, null, null);
    }

    @Override
    public String toString() {
      return "JobResult{" +
              "id='" + id + '\'' +
              ", result=" + result +
              ", error=" + error +
              ", sparkCounters=" + sparkCounters +
              '}';
    }
  }

  protected static class JobStarted implements Serializable {

    final String id;

    JobStarted(String id) {
      this.id = id;
    }

    JobStarted() {
      this(null);
    }

    @Override
    public String toString() {
      return "JobStarted{" +
              "id='" + id + '\'' +
              '}';
    }
  }

  /**
   * Inform the client that a new spark job has been submitted for the client job.
   */
  protected static class JobSubmitted implements Serializable {
    final String clientJobId;
    final int sparkJobId;

    JobSubmitted(String clientJobId, int sparkJobId) {
      this.clientJobId = clientJobId;
      this.sparkJobId = sparkJobId;
    }

    JobSubmitted() {
      this(null, -1);
    }

    @Override
    public String toString() {
      return "JobSubmitted{" +
              "clientJobId='" + clientJobId + '\'' +
              ", sparkJobId=" + sparkJobId +
              '}';
    }
  }

  protected static class SyncJobRequest<T extends Serializable> implements Serializable {

    final Job<T> job;

    SyncJobRequest(Job<T> job) {
      this.job = job;
    }

    SyncJobRequest() {
      this(null);
    }

    @Override
    public String toString() {
      return "SyncJobRequest{" +
              "job=" + job +
              '}';
    }
  }

  protected static class RunCommand implements Serializable {

    final String command;
    final byte[] hiveConfBytes;
    final String queryId;

    RunCommand(String command, byte[] hiveConfBytes, String queryId) {
      this.command = command;
      this.hiveConfBytes = hiveConfBytes;
      this.queryId = queryId;
    }
  }

  protected static class StartSession implements Serializable {

    final byte[] hiveConfBytes;

    StartSession(byte[] hiveConfBytes) {
      this.hiveConfBytes = hiveConfBytes;
    }
  }

  protected static class HasResultSet implements Serializable {

    final String queryId;

    HasResultSet(String queryId) {
      this.queryId = queryId;
    }
  }

  protected static class GetSchema implements Serializable {

    final String queryId;

    GetSchema(String queryId) {
      this.queryId = queryId;
    }
  }

  protected static class IsFetchingTable implements Serializable {

    final String queryId;

    IsFetchingTable(String queryId) {
      this.queryId = queryId;
    }
  }

  protected static class CloseDriverRequest implements Serializable {

    final String queryId;

    CloseDriverRequest(String queryId) {
      this.queryId = queryId;
    }
  }

  protected static class DestroyDriverRequest implements Serializable {

    final String queryId;

    DestroyDriverRequest(String queryId) {
      this.queryId = queryId;
    }
  }

  protected static class GetSchemaResponse implements Serializable {

    final String queryId;
    final byte[] schema;

    GetSchemaResponse(String queryId, byte[] schema) {
      this.queryId = queryId;
      this.schema = schema;
    }
  }

  protected static class HasResultSetResponse implements Serializable {

    final String queryId;
    final boolean hasResultSet;

    HasResultSetResponse(String queryId, boolean hasResultSet) {
      this.queryId = queryId;
      this.hasResultSet = hasResultSet;
    }
  }

  protected static class IsFetchingTableResponse implements Serializable {

    final String queryId;
    final boolean isFetchingTableResponse;

    IsFetchingTableResponse(String queryId, boolean isFetchingTableResponse) {
      this.queryId = queryId;
      this.isFetchingTableResponse = isFetchingTableResponse;
    }
  }

  protected static class CompileCommand implements Serializable {

    final String command;
    final byte[] hiveConfBytes;
    final String queryId;

    CompileCommand(String command, byte[] hiveConfBytes, String queryId) {
      this.command = command;
      this.hiveConfBytes = hiveConfBytes;
      this.queryId = queryId;
    }
  }

  protected static class GetResults implements Serializable {

    final String queryId;

    GetResults(String queryId) {
      this.queryId = queryId;
    }
  }

  protected static class CommandResults implements Serializable {

    final List res;
    final String queryId;
    final boolean moreResults;

    CommandResults(List res, String queryId, boolean moreResults) {
      this.res = res;
      this.queryId = queryId;
      this.moreResults = moreResults;
    }
  }

  protected static class CommandProcessorResponseMessage implements Serializable {

    final String queryId;
    final Exception commandProcessorResponse;

    CommandProcessorResponseMessage(String queryId, Exception commandProcessorResponse) {
      this.queryId = queryId;
      this.commandProcessorResponse = commandProcessorResponse;
    }
  }
}
