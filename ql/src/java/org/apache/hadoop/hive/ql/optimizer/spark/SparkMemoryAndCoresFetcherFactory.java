package org.apache.hadoop.hive.ql.optimizer.spark;

import org.apache.hadoop.hive.conf.HiveConf;

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
