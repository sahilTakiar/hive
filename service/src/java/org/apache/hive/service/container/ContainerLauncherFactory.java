package org.apache.hive.service.container;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.container.spark.SparkContainerLauncher;

public class ContainerLauncherFactory {

  public static ContainerLauncher getContainerLauncher(HiveConf hiveConf) {
    if ("spark".equals(HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE))) {
      // Need to pass in a Map<String, String> conf
      return new SparkContainerLauncher(null, hiveConf);
    }
    return null;
  }
}
