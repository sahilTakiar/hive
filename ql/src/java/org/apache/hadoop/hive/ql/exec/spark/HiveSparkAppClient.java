package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.spark.SparkConf;

import java.io.Closeable;

public interface HiveSparkAppClient extends Closeable {

  /**
   * @return spark configuration
   */
  SparkConf getSparkConf();

  /**
   * @return the number of executors
   */
  int getExecutorCount() throws Exception;

  /**
   * For standalone mode, this can be used to get total number of cores.
   * @return  default parallelism.
   */
  int getDefaultParallelism() throws Exception;
}
