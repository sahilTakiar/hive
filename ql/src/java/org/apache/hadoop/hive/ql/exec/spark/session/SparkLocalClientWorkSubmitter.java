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

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.spark.LocalHiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.JobMetricsListener;
import org.apache.hadoop.hive.ql.plan.SparkWork;

import org.apache.hive.spark.client.RemoteDriver;


/**
 * A {@link SparkWorkSubmitter} that submits {@link SparkWork} objects running them locally via a
 * {@link LocalHiveSparkClient}.
 */
class SparkLocalClientWorkSubmitter implements SparkWorkSubmitter {

  @Override
  public SparkJobRef submit(DriverContext driverContext, SparkWork sparkWork) throws Exception {
    JobMetricsListener jobMetricsListener = new JobMetricsListener();
    RemoteDriver.getInstance().sc().sc().addSparkListener(jobMetricsListener);
    return new LocalHiveSparkClient(RemoteDriver.getInstance().sc(), jobMetricsListener).execute(driverContext, sparkWork);
  }

  @Override
  public int close(SparkTask sparkTask, int rc) {
    return sparkTask.close(rc);
  }
}
