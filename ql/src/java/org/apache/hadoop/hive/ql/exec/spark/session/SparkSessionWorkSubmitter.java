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

package org.apache.hadoop.hive.ql.exec.spark.session;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.SparkWork;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link SparkWorkSubmitter} that submits {@link SparkWork} object using the
 * {@link SparkSession#submit(DriverContext, SparkWork)} method.
 */
class SparkSessionWorkSubmitter implements SparkWorkSubmitter {

  private static final Logger LOG = LoggerFactory.getLogger(SparkSessionWorkSubmitter.class);

  private final HiveConf conf;

  private SparkSessionManager sparkSessionManager;
  private SparkSession sparkSession;


  public SparkSessionWorkSubmitter(HiveConf conf) {
    this.conf = conf;
  }

  @Override
  public SparkJobRef submit(DriverContext driverContext, SparkWork sparkWork) throws Exception {
    this.sparkSessionManager = SparkSessionManagerImpl.getInstance();
    this.sparkSession = SparkUtilities.getSparkSession(conf, sparkSessionManager);
    return this.sparkSession.submit(driverContext, sparkWork);
  }

  @Override
  public int close(SparkTask sparkTask, int rc) {
    if (sparkSession != null && sparkSessionManager != null) {
      rc = sparkTask.close(rc);
      try {
        sparkSessionManager.returnSession(sparkSession);
      } catch (HiveException ex) {
        LOG.error("Failed to return the session to SessionManager", ex);
      }
    }
    return rc;
  }
}
