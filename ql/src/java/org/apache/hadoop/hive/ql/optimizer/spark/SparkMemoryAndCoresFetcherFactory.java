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

import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Creates {@link SparkMemoryAndCoresFetcher}s.
 */
public class SparkMemoryAndCoresFetcherFactory {

  private final HiveConf hiveConf;

  SparkMemoryAndCoresFetcherFactory(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  public SparkMemoryAndCoresFetcher createSparkMemoryAndCoresFetcher() {
    if (this.hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_CONTAINER_SERVICE)) {
      return new SparkRemoteDriverMemoryAndCoresFetcher(this.hiveConf);
    } else {
      return new SparkSessionMemoryAndCoresFetcher(this.hiveConf);
    }
  }
}
