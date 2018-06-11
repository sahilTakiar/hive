package org.apache.hive.spark.client;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.DriverProtocol;

import java.util.Map;

public class DriverProtocolFactory {

  public static DriverProtocol getDriverProtocol(Map<String, String> mapConf, RemoteDriver remoteDriver) {
    if (Boolean.getBoolean(mapConf.get(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_CONTAINER_SERVICE
            .varname))) {
      // TODO
      return null;
    } else {
      return new DriverProtocol(remoteDriver);
    }
  }
}
