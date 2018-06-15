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

public class SparkSessionWorkSubmitter implements SparkWorkSubmitter {

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
