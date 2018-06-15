package org.apache.hadoop.hive.ql.optimizer.spark;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManager;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkSessionMemoryAndCoresFetcher implements SparkMemoryAndCoresFetcher {

  private static final Logger LOG = LoggerFactory.getLogger(
          SparkSessionMemoryAndCoresFetcher.class);

  private final HiveConf hiveConf;

  SparkSessionMemoryAndCoresFetcher(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public ObjectPair<Long, Integer> getSparkMemoryAndCores() throws SemanticException {
    SparkSessionManager sparkSessionManager = null;
    SparkSession sparkSession = null;
    try {
      sparkSessionManager = SparkSessionManagerImpl.getInstance();
      sparkSession = SparkUtilities.getSparkSession(this.hiveConf, sparkSessionManager);
      return sparkSession.getMemoryAndCores();
    } catch (HiveException e) {
      throw new SemanticException("Failed to get a Hive on Spark session", e);
    } catch (Exception e) {
      LOG.warn("Failed to get spark memory/core info, reducer parallelism may be inaccurate", e);
    } finally {
      if (sparkSession != null && sparkSessionManager != null) {
        try {
          sparkSessionManager.returnSession(sparkSession);
        } catch (HiveException ex) {
          LOG.error("Failed to return the session to SessionManager: " + ex, ex);
        }
      }
    }
    return null;
  }
}
