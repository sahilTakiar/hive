package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.KryoSerializer;
import org.apache.hadoop.hive.ql.exec.spark.RemoteHiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;


class SparkRemoteProcessLauncher implements RemoteProcessLauncher {

  private final HiveConf hiveConf;

  SparkRemoteProcessLauncher(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public void launch() throws HiveException {
    SparkSession ss = SparkUtilities.getSparkSession(hiveConf, SparkSessionManagerImpl.getInstance());
    byte[] hiveConfBytes = KryoSerializer.serializeHiveConf(hiveConf);
    ((RemoteHiveSparkClient) ss.getHiveSparkClient()).getSparkClient().getClientProtocol().startSession(hiveConfBytes);
  }
}
