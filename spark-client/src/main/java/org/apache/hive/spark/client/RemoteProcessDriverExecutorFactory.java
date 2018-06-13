package org.apache.hive.spark.client;

import org.apache.hive.spark.client.rpc.Rpc;

public interface RemoteProcessDriverExecutorFactory {

  RemoteProcessDriverExecutor createRemoteProcessDriverExecutor(String command,
                                                                byte[] hiveConfBytes,
                                                                String queryId);
}
