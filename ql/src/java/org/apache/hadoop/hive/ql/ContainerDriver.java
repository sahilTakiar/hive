package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.processors.CommandProcessor;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;

// TODO should return CommanProcessorResponse instead of throwing exceptions
public class ContainerDriver implements CommandProcessor {

  private final HiveConf hiveConf;

  public ContainerDriver(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
  }

  @Override
  public CommandProcessorResponse run(String command) {
    ContainerServiceClient client = SessionState.get().getContainerServiceClient();
    if (client == null) {
      try {
        client = ContainerLauncherFactory.getContainerLauncher(hiveConf).launch();
        SessionState.get().setContainerServiceClient(client);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    try {
      client.execute(command);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  @Override
  public void close() throws Exception {

  }
}
