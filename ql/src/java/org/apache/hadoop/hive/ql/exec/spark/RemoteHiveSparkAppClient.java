package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.spark.client.SparkClient;
import org.apache.hive.spark.client.SparkClientFactory;
import org.apache.hive.spark.client.SparkClientUtilities;

import org.apache.spark.SparkConf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RemoteHiveSparkAppClient implements HiveSparkAppClient {

  private static final transient Logger LOG = LoggerFactory.getLogger(RemoteHiveSparkClient.class);

  private transient Map<String, String> conf;
  transient SparkClient remoteClient;
  private transient SparkConf sparkConf;
  transient HiveConf hiveConf;

  private final transient long sparkClientTimtout;
  private final String sessionId;

  RemoteHiveSparkAppClient(HiveConf hiveConf, Map<String, String> conf, String sessionId) throws Exception {
    this.hiveConf = hiveConf;
    sparkClientTimtout = hiveConf.getTimeVar(HiveConf.ConfVars.SPARK_CLIENT_FUTURE_TIMEOUT,
        TimeUnit.SECONDS);
    sparkConf = HiveSparkAppClientFactory.generateSparkConf(conf);
    this.conf = conf;
    this.sessionId = sessionId;
    createRemoteClient();
  }

  private void createRemoteClient() throws Exception {
    remoteClient = SparkClientFactory.createClient(conf, hiveConf, sessionId);

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_PREWARM_ENABLED) &&
            (SparkClientUtilities.isYarnMaster(hiveConf.get("spark.master")) ||
             SparkClientUtilities.isLocalMaster(hiveConf.get("spark.master")))) {
      int minExecutors = getExecutorsToWarm();
      if (minExecutors <= 0) {
        return;
      }

      LOG.info("Prewarm Spark executors. The minimum number of executors to warm is " + minExecutors);

      // Spend at most HIVE_PREWARM_SPARK_TIMEOUT to wait for executors to come up.
      int curExecutors = 0;
      long maxPrewarmTime = HiveConf.getTimeVar(hiveConf, HiveConf.ConfVars.HIVE_PREWARM_SPARK_TIMEOUT,
          TimeUnit.MILLISECONDS);
      long ts = System.currentTimeMillis();
      do {
        try {
          curExecutors = getExecutorCount(maxPrewarmTime, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
          // let's don't fail on future timeout since we have a timeout for pre-warm
          LOG.warn("Timed out getting executor count.", e);
        }
        if (curExecutors >= minExecutors) {
          LOG.info("Finished prewarming Spark executors. The current number of executors is " + curExecutors);
          return;
        }
        Thread.sleep(500); // sleep half a second
      } while (System.currentTimeMillis() - ts < maxPrewarmTime);

      LOG.info("Timeout (" + maxPrewarmTime / 1000 + "s) occurred while prewarming executors. " +
          "The current number of executors is " + curExecutors);
    }
  }

  /**
   * Please note that the method is very tied with Spark documentation 1.4.1 regarding
   * dynamic allocation, such as default values.
   * @return
   */
  private int getExecutorsToWarm() {
    int minExecutors =
        HiveConf.getIntVar(hiveConf, HiveConf.ConfVars.HIVE_PREWARM_NUM_CONTAINERS);
    boolean dynamicAllocation = hiveConf.getBoolean("spark.dynamicAllocation.enabled", false);
    if (dynamicAllocation) {
      int min = sparkConf.getInt("spark.dynamicAllocation.minExecutors", 0);
      int initExecutors = sparkConf.getInt("spark.dynamicAllocation.initialExecutors", min);
      minExecutors = Math.min(minExecutors, initExecutors);
    } else {
      int execInstances = sparkConf.getInt("spark.executor.instances", 2);
      minExecutors = Math.min(minExecutors, execInstances);
    }
    return minExecutors;
  }

  private int getExecutorCount(long timeout, TimeUnit unit) throws Exception {
    Future<Integer> handler = remoteClient.getExecutorCount();
    return handler.get(timeout, unit);
  }

  @Override
  public SparkConf getSparkConf() {
    return sparkConf;
  }

  @Override
  public int getExecutorCount() throws Exception {
    return getExecutorCount(sparkClientTimtout, TimeUnit.SECONDS);
  }

  @Override
  public int getDefaultParallelism() throws Exception {
    Future<Integer> handler = remoteClient.getDefaultParallelism();
    return handler.get(sparkClientTimtout, TimeUnit.SECONDS);
  }

  public void close() {
    if (remoteClient != null) {
      remoteClient.stop();
    }
  }
}
