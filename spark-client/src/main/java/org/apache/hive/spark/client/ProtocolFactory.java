package org.apache.hive.spark.client;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.spark.client.rpc.RpcDispatcher;

import java.util.Map;

public class ProtocolFactory {

  public static RpcDispatcher getClientProtocol(HiveConf hiveConf) {
    return null;
  }

  public static RpcDispatcher getDriverProtocol(Map<String, String> conf) {
    return null;
  }
}
