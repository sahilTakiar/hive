package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.JobMetricsListener;
import org.apache.hive.spark.client.SparkClientUtilities;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;

public class LocalHiveSparkAppClient implements HiveSparkAppClient {

  protected static final transient Logger LOG = LoggerFactory.getLogger(LocalHiveSparkClient.class);

  private static LocalHiveSparkAppClient client;

  public static synchronized LocalHiveSparkAppClient getInstance(
      SparkConf sparkConf, HiveConf hiveConf) throws FileNotFoundException, MalformedURLException {
    if (client == null) {
      client = new LocalHiveSparkAppClient(sparkConf, hiveConf);
    }
    return client;
  }

  private final JavaSparkContext sc;

  private final JobMetricsListener jobMetricsListener;

  private LocalHiveSparkAppClient(SparkConf sparkConf, HiveConf hiveConf)
      throws FileNotFoundException, MalformedURLException {
    String regJar = null;
    // the registrator jar should already be in CP when not in test mode
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_IN_TEST)) {
      String kryoReg = sparkConf.get("spark.kryo.registrator", "");
      if (SparkClientUtilities.HIVE_KRYO_REG_NAME.equals(kryoReg)) {
        regJar = SparkClientUtilities.findKryoRegistratorJar(hiveConf);
        SparkClientUtilities.addJarToContextLoader(new File(regJar));
      }
    }
    sc = new JavaSparkContext(sparkConf);
    if (regJar != null) {
      sc.addJar(regJar);
    }
    jobMetricsListener = new JobMetricsListener();
    sc.sc().addSparkListener(jobMetricsListener);
  }

  public void close() {
    synchronized (LocalHiveSparkClient.class) {
      client = null;
    }
    if (sc != null) {
      sc.stop();
    }
  }

  @Override
  public SparkConf getSparkConf() {
    return null;
  }

  @Override
  public int getExecutorCount() {
    return 0;
  }

  @Override
  public int getDefaultParallelism() {
    return 0;
  }

  JavaSparkContext getSc() {
    return this.sc;
  }

  JobMetricsListener getJobMetricsListener() {
    return this.jobMetricsListener;
  }
}
