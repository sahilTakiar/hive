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
import org.apache.hadoop.hive.ql.exec.spark.KryoSerializer;
import org.apache.hadoop.hive.ql.exec.spark.RemoteProcessHiveSparkClient;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.util.List;


/**
 * A {@link RemoteProcessClient} that uses a Spark driver to run all remote driver operations. It
 * uses a {@link RemoteProcessHiveSparkClient} to interact with the remote driver.
 */
class SparkRemoteProcessClient implements RemoteProcessClient {

  private final String queryId;
  private final HiveConf hiveConf;
  private final RemoteProcessHiveSparkClient remoteProcessHiveSparkClient;
  private PerfLogger perfLogger;

  SparkRemoteProcessClient(String queryId, HiveConf hiveConf,
                           RemoteProcessHiveSparkClient remoteProcessHiveSparkClient) {
    this.queryId = queryId;
    this.hiveConf = hiveConf;
    this.remoteProcessHiveSparkClient = remoteProcessHiveSparkClient;
    this.perfLogger = SessionState.getPerfLogger();
  }

  @Override
  public CommandProcessorResponse run(String statement) {
    this.perfLogger.PerfLogBegin(getClass().getSimpleName(), "serializeHiveConf");
    byte[] hiveConfBytes = KryoSerializer.serializeHiveConf(hiveConf);
    this.perfLogger.PerfLogEnd(getClass().getSimpleName(), "serializeHiveConf");

    return this.remoteProcessHiveSparkClient.run(statement, hiveConfBytes);
  }

  @Override
  public boolean getResults(List res) throws IOException {
    return this.remoteProcessHiveSparkClient.getResults(res);
  }

  @Override
  public CommandProcessorResponse compileAndRespond(String statement) {
    this.perfLogger.PerfLogBegin(getClass().getSimpleName(), "serializeHiveConf");
    byte[] hiveConfBytes = KryoSerializer.serializeHiveConf(hiveConf);
    this.perfLogger.PerfLogEnd(getClass().getSimpleName(), "serializeHiveConf");
    return this.remoteProcessHiveSparkClient.compileAndRespond(statement, hiveConfBytes);
  }

  @Override
  public CommandProcessorResponse run() {
    return this.remoteProcessHiveSparkClient.run();
  }

  @Override
  public boolean hasResultSet() {
    return this.remoteProcessHiveSparkClient.hasResultSet();
  }

  @Override
  public Schema getSchema() {
    return this.remoteProcessHiveSparkClient.getSchema();
  }

  @Override
  public boolean isFetchingTable() {
    return this.remoteProcessHiveSparkClient.isFetchingTable();
  }

  @Override
  public void close() {
    this.remoteProcessHiveSparkClient.close();
  }

  @Override
  public void destroy() {
    this.remoteProcessHiveSparkClient.destroy();
  }

  @Override
  public QueryDisplay getQueryDisplay() {
    return this.remoteProcessHiveSparkClient.getQueryDisplay();
  }
}
