package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;


public class SparkContainerLauncher implements ContainerLauncher {

  private final HiveConf hiveConf;

  public SparkContainerLauncher(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public ContainerServiceClient launch() {
    SparkSession sparkSession;
    try {
      sparkSession = SparkUtilities.getSparkSession(hiveConf, SparkSessionManagerImpl.getInstance());
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
    return new SparkContainerServiceClient(sparkSession);
  }
}
