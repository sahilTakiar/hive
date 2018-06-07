package org.apache.hadoop.hive.ql.exec.spark.session;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.plan.SparkWork;

public class SparkSessionWorkSubmitter implements SparkWorkSubmitter {

  private final HiveConf conf;

  public SparkSessionWorkSubmitter(HiveConf conf) {
    this.conf = conf;
  }

  @Override
  public SparkJobRef submit(DriverContext driverContext, SparkWork sparkWork) throws Exception {
    SparkSessionManager sparkSessionManager = SparkSessionManagerImpl.getInstance();
    SparkSession sparkSession = SparkUtilities.getSparkSession(conf, sparkSessionManager);
    return sparkSession.submit(driverContext, sparkWork);
  }
}
