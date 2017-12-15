package org.apache.hive.service.container;

import org.apache.hive.service.container.thrift.ThriftContainerServiceClient;

import java.io.IOException;

public interface ContainerLauncher {

  void launch() throws IOException;

  ThriftContainerServiceClient getClient();
}
