package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.RemoteHiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.RemoteProcessHiveSparkClientImpl;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class RemoteProcessClientFactory {

  public static RemoteProcessClient createRemoteProcessClient(HiveConf hiveConf, String queryId) {

    if ("spark".equals(HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE))) {
      SparkSession sparkSession;
      try {
        sparkSession = SparkUtilities.getSparkSession(hiveConf,
                SparkSessionManagerImpl.getInstance());
      } catch (HiveException e) {
        throw new RuntimeException(e);
      }
      if (!(sparkSession.getHiveSparkClient() instanceof RemoteHiveSparkClient)) {
        throw new IllegalArgumentException();
      }
      RemoteHiveSparkClient remoteHiveSparkClient = (RemoteHiveSparkClient) sparkSession.getHiveSparkClient();
      return new SparkRemoteProcessClient(queryId, hiveConf, new RemoteProcessHiveSparkClientImpl(queryId, remoteHiveSparkClient.getSparkClient().getClientProtocol()));
    }
    throw new IllegalArgumentException();
  }
}
