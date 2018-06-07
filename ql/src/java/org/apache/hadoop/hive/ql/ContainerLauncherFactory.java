package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;

public class ContainerLauncherFactory {

  public static ContainerLauncher getContainerLauncher(HiveConf hiveConf) {
    if ("spark".equals(HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE))) {
      return new SparkContainerLauncher(hiveConf);
    }
    throw new IllegalArgumentException();
  }
}