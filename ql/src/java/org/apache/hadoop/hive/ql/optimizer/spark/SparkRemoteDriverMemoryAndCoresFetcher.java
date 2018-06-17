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
package org.apache.hadoop.hive.ql.optimizer.spark;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.HiveSparkClientFactory;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hive.spark.client.RemoteDriver;

import org.apache.spark.SparkConf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SparkRemoteDriverMemoryAndCoresFetcher implements SparkMemoryAndCoresFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(
          SparkRemoteDriverMemoryAndCoresFetcher.class);

  private final HiveConf hiveConf;

  public SparkRemoteDriverMemoryAndCoresFetcher(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public ObjectPair<Long, Integer> getSparkMemoryAndCores() {
    int numExecutors = RemoteDriver.getInstance().sc().sc().getExecutorMemoryStatus().size() - 1;
    int defaultParallelism = RemoteDriver.getInstance().sc().sc().defaultParallelism();
    SparkConf sparkConf = HiveSparkClientFactory.generateSparkConf(
            HiveSparkClientFactory.initiateSparkConf(this.hiveConf, null));
    return SparkUtilities.getMemoryAndCores(LOG, sparkConf, numExecutors, defaultParallelism);
  }
}
