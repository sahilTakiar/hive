package org.apache.hadoop.hive.ql.exec.spark.session;

import org.apache.hadoop.hive.conf.HiveConf;

public class SparkWorkSubmitterFactory {

  public static SparkWorkSubmitter getSparkWorkSubmitter(HiveConf hiveConf) {
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_CONTAINER_SERVICE)) {
      return new SparkLocalClientWorkSubmitter();
    } else {
      return new SparkSessionWorkSubmitter(hiveConf);
    }
  }
}
