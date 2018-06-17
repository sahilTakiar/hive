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
import org.apache.hadoop.hive.ql.exec.spark.KryoSerializer;
import org.apache.hadoop.hive.ql.exec.spark.RemoteHiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;


/**
 * A {@link RemoteProcessLauncher} that defines the remote process as the Spark driver. It uses
 * the existing {@link SparkSession} and {@link org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager}
 * logic to launch the {@link org.apache.hive.spark.client.RemoteDriver}.
 */
class SparkRemoteProcessLauncher implements RemoteProcessLauncher {

  private final HiveConf hiveConf;

  SparkRemoteProcessLauncher(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public void launch() throws HiveException {
    SparkSession ss = SparkUtilities.getSparkSession(hiveConf, SparkSessionManagerImpl.getInstance());
    byte[] hiveConfBytes = KryoSerializer.serializeHiveConf(hiveConf);
    ((RemoteHiveSparkClient) ss.getHiveSparkClient()).getSparkClient().getClientProtocol().startSession(hiveConfBytes);
  }
}
