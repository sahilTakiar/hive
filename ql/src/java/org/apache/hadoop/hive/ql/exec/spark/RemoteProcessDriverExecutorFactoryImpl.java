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

package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.spark.client.RemoteProcessDriverExecutor;
import org.apache.hive.spark.client.RemoteProcessDriverExecutorFactory;

import java.io.IOException;
import java.util.List;


/**
 * A simple of implementation of {@link RemoteProcessDriverExecutorFactory} that creates
 * {@link RemoteProcessDriverExecutor}s.
 */
public class RemoteProcessDriverExecutorFactoryImpl implements RemoteProcessDriverExecutorFactory {

  private final HiveConf hiveConf;

  public RemoteProcessDriverExecutorFactoryImpl(byte[] hiveConfBytes) {
    this.hiveConf = KryoSerializer.deserializeHiveConf(hiveConfBytes);
    SessionState.start(this.hiveConf);
  }

  @Override
  public RemoteProcessDriverExecutor createRemoteProcessDriverExecutor(byte[] hiveConfBytes) {
    return new RemoteProcessDriverExecutorImpl(hiveConfBytes);
  }

  /**
   * A simple implementation of {@link RemoteProcessDriverExecutor} that delegates to the
   * {@link IDriver} defined by {@link DriverFactory#newDriver(HiveConf)}.
   */
  private static final class RemoteProcessDriverExecutorImpl implements RemoteProcessDriverExecutor {

    private HiveConf hiveConf;
    private IDriver driver;

    private RemoteProcessDriverExecutorImpl(byte[] hiveConfBytes) {
      this.hiveConf = KryoSerializer.deserializeHiveConf(hiveConfBytes);
      this.driver = DriverFactory.newDriver(this.hiveConf);
    }

    @Override
    public Exception run(String command) {
      return this.driver.run(command);
    }

    @Override
    public boolean getResults(List res) throws IOException {
      return this.driver.getResults(res);
    }

    @Override
    public Exception run() {
      return this.driver.run();
    }

    @Override
    public Exception compileAndRespond(String command) {
      return this.driver.compileAndRespond(command);
    }

    @Override
    public boolean hasResultSet() {
      return this.driver.hasResultSet();
    }

    @Override
    public byte[] getSchema() {
      return KryoSerializer.serialize(this.driver.getSchema());
    }

    @Override
    public boolean isFetchingTable() {
      return this.driver.isFetchingTable();
    }

    @Override
    public void close() {
      this.driver.close();
    }

    @Override
    public void destroy() {
      this.driver.destroy();
    }

    @Override
    public byte[] getQueryDisplay() {
      return KryoSerializer.serialize(this.driver.getQueryDisplay());
    }
  }
}
