package org.apache.hadoop.hive.ql;

import java.io.IOException;

public interface ContainerLauncher {

  ContainerServiceClient launch() throws IOException;
}
