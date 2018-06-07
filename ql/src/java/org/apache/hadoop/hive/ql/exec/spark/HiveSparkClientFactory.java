/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.compress.utils.CharsetNames;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.common.LogUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.spark.client.SparkClientUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.spark.SparkConf;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;

public class HiveSparkClientFactory {
  protected static final transient Logger LOG = LoggerFactory.getLogger(HiveSparkClientFactory.class);

  private static final String SPARK_DEFAULT_CONF_FILE = "spark-defaults.conf";
  private static final String SPARK_DEFAULT_MASTER = "yarn";
  private static final String SPARK_DEFAULT_DEPLOY_MODE = "cluster";
  private static final String SPARK_DEFAULT_APP_NAME = "Hive on Spark";
  private static final String SPARK_DEFAULT_SERIALIZER = "org.apache.spark.serializer.KryoSerializer";
  private static final String SPARK_DEFAULT_REFERENCE_TRACKING = "false";
  private static final String SPARK_WAIT_APP_COMPLETE = "spark.yarn.submit.waitAppCompletion";
  private static final String SPARK_DEPLOY_MODE = "spark.submit.deployMode";
  @VisibleForTesting
  public static final String SPARK_CLONE_CONFIGURATION = "spark.hadoop.cloneConf";

  public static HiveSparkClient createHiveSparkClient(HiveSparkAppClient sparkAppClient) throws Exception {

    // Submit spark job through local spark context while spark master is local mode, otherwise submit
    // spark job through remote spark context.
    String master = sparkAppClient.getSparkConf().get("spark.master");
    if (master.equals("local") || master.startsWith("local[")) {
      // With local spark context, all user sessions share the same spark context.
      return new LocalHiveSparkClient((LocalHiveSparkAppClient) sparkAppClient);
    } else {
      return new RemoteHiveSparkClient((RemoteHiveSparkAppClient) sparkAppClient);
    }
  }
}
