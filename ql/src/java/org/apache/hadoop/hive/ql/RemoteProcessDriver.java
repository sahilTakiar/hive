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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.util.List;


/**
 * Runs a {@link IDriver} in a remote process.
 */
public class RemoteProcessDriver implements IDriver {

  private final HiveConf hiveConf;
  private final QueryState queryState;
  private final String userName;
  private final QueryInfo queryInfo;
  private final RemoteProcessClient remoteProcessClient;

  public RemoteProcessDriver(HiveConf hiveConf) throws IOException, HiveException {
    this(new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(hiveConf).build(), null,
            null);
  }

  public RemoteProcessDriver(QueryState queryState, String userName, QueryInfo queryInfo) throws IOException, HiveException {
    this.hiveConf = queryState.getConf();
    this.queryState = queryState;
    this.userName = userName;
    this.queryInfo = queryInfo;
    this.remoteProcessClient = createRemoteProcessClient(queryState.getConf(), queryState.getQueryId());
  }

  @Override
  public int compile(String string) {
    return 0;
  }

  @Override
  public CommandProcessorResponse compileAndRespond(String statement) {
    return this.remoteProcessClient.compileAndRespond(statement);
  }

  @Override
  public QueryPlan getPlan() {
    return null;
  }

  @Override
  public QueryDisplay getQueryDisplay() {
    return new QueryDisplay() {
      @Override
      public synchronized List<TaskDisplay> getTaskDisplays() {
        return null;
      }
    };
  }

  @Override
  public void setOperationId(String guid64) {

  }

  @Override
  public CommandProcessorResponse run() {
    return this.remoteProcessClient.run();
  }

  @Override
  public CommandProcessorResponse run(String command) {
    try {
      return this.remoteProcessClient.run(command);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean getResults(List res) throws IOException {
    return this.remoteProcessClient.getResults(res);
  }

  @Override
  public void setMaxRows(int maxRows) {

  }

  @Override
  public FetchTask getFetchTask() {
    return null;
  }

  @Override
  public Schema getSchema() {
    return this.remoteProcessClient.getSchema();
  }

  @Override
  public boolean isFetchingTable() {
    return this.remoteProcessClient.isFetchingTable();
  }

  @Override
  public void resetFetch() {

  }

  @Override
  public void close() {
    this.remoteProcessClient.close();
  }

  @Override
  public void destroy() {
    this.remoteProcessClient.destroy();
  }

  @Override
  public HiveConf getConf() {
    return null;
  }

  /**
   * Don't support getting the {@link Context} because it requires serializing the entire context
   * object. This method is mostly used for the {@link org.apache.hadoop.hive.ql.reexec.ReExecDriver}
   * and various unit tests.
   */
  @Override
  public Context getContext() {
    throw new UnsupportedOperationException(
            "RemoteProcessDriver does not support getting the Semantic Analyzer Context");
  }

  @Override
  public boolean hasResultSet() {
    return this.remoteProcessClient.hasResultSet();
  }

  private RemoteProcessClient createRemoteProcessClient(HiveConf hiveConf,
                                                        String queryId) throws IOException, HiveException {
    SessionState.get().launchRemoteProcess();
    return RemoteProcessClientFactory.createRemoteProcessClient(hiveConf, queryId);
  }
}
