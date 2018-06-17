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
import org.apache.hadoop.hive.ql.exec.spark.RemoteHiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.RemoteProcessHiveSparkClientImpl;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Creates a {@link RemoteProcessClient} for a {@link RemoteProcessDriver}.
 */
public class RemoteProcessClientFactory {

  public static RemoteProcessClient createRemoteProcessClient(HiveConf hiveConf, String queryId) {

    if ("spark".equals(HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE))) {
      SparkSession sparkSession;
      try {
        sparkSession = SparkUtilities.getSparkSession(hiveConf,
                SparkSessionManagerImpl.getInstance());
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
      if (!(sparkSession.getHiveSparkClient() instanceof RemoteHiveSparkClient)) {
        throw new IllegalArgumentException();
      }
      RemoteHiveSparkClient remoteHiveSparkClient = (RemoteHiveSparkClient) sparkSession.getHiveSparkClient();
      return new SparkRemoteProcessClient(queryId, hiveConf, new RemoteProcessHiveSparkClientImpl(queryId, remoteHiveSparkClient.getSparkClient().getClientProtocol()));
    }
    throw new IllegalArgumentException();
  }
}
