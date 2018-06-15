package org.apache.hadoop.hive.ql.optimizer.spark;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public interface SparkMemoryAndCoresFetcher {

  ObjectPair<Long, Integer> getSparkMemoryAndCores() throws SemanticException;
}
