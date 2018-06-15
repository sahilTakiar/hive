package org.apache.hadoop.hive.ql.optimizer.spark;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.HiveSparkClientFactory;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hive.spark.client.RemoteDriver;

import org.apache.spark.SparkConf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkRemoteDriverMemoryAndCoresFetcher implements SparkMemoryAndCoresFetcher {

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
