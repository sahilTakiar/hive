package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;

public class RemoteProcessLauncherFactory {

  public static RemoteProcessLauncher getRemoteProcessLauncher(HiveConf hiveConf) {
    if ("spark".equals(HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE))) {
      return new SparkRemoteProcessLauncher(hiveConf);
    }
    throw new IllegalArgumentException();
  }
}