package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.RemoteHiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.RemoteProcessHiveSparkClientImpl;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;


public class SparkRemoteProcessLauncher implements RemoteProcessLauncher {

  private final HiveConf hiveConf;

  public SparkRemoteProcessLauncher(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public RemoteProcessClient launch() {
    SparkSession sparkSession;
    try {
      sparkSession = SparkUtilities.getSparkSession(hiveConf, SparkSessionManagerImpl.getInstance());
    } catch (HiveException e) {
      throw new RuntimeException(e);
    }
    if (!(sparkSession.getHiveSparkClient() instanceof RemoteHiveSparkClient)) {
      throw new IllegalArgumentException();
    }
    RemoteHiveSparkClient remoteHiveSparkClient = (RemoteHiveSparkClient) sparkSession.getHiveSparkClient();
    return new SparkRemoteProcessClient(this.hiveConf, new RemoteProcessHiveSparkClientImpl(remoteHiveSparkClient.getSparkClient().getClientProtocol()));
  }
}
