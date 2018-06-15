package org.apache.hadoop.hive.ql.exec.spark.session;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.spark.LocalHiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.JobMetricsListener;
import org.apache.hadoop.hive.ql.plan.SparkWork;

import org.apache.hive.spark.client.RemoteDriver;

public class SparkLocalClientWorkSubmitter implements SparkWorkSubmitter {

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
