package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.util.List;


/**
 * Runs a {@link IDriver} in a remote process
 *
 * TODO this is really where the high level client-protocol should be defined
 */
public class RemoteDriver implements IDriver {

  private final IDriver driver;

  public RemoteDriver(IDriver driver) {
    this.driver = driver;
  }

  @Override
  public int compile(String string) {
    return 0;
  }

  @Override
  public CommandProcessorResponse compileAndRespond(String statement) {
    return null;
  }

  @Override
  public QueryPlan getPlan() {
    return null;
  }

  @Override
  public QueryDisplay getQueryDisplay() {
    return null;
  }

  @Override
  public void setOperationId(String guid64) {

  }

  @Override
  public CommandProcessorResponse run() {
    return null;
  }

  @Override
  public CommandProcessorResponse run(String command) {
    // TODO proper handling of CommandProcessorResponse
    ContainerServiceClient client = SessionState.get().getContainerServiceClient();
    if (client == null) {
      try {
        client = ContainerLauncherFactory.getContainerLauncher(this.driver.getConf()).launch();
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
  public boolean getResults(List res) throws IOException {
    return false;
  }

  @Override
  public void setMaxRows(int maxRows) {

  }

  @Override
  public FetchTask getFetchTask() {
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }

  @Override
  public boolean isFetchingTable() {
    return false;
  }

  @Override
  public void resetFetch() throws IOException {

  }

  @Override
  public void close() {

  }

  @Override
  public void destroy() {

  }

  @Override
  public HiveConf getConf() {
    return null;
  }

  @Override
  public Context getContext() {
    return null;
  }

  @Override
  public boolean hasResultSet() {
    return false;
  }
}
