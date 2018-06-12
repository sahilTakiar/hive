package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.util.List;


/**
 * Runs a {@link IDriver} in a remote process.
 */
public class RemoteProcessDriver implements IDriver {

  private final HiveConf hiveConf;
  private final QueryState queryState;
  private final String userName;
  private final QueryInfo queryInfo;

  public RemoteProcessDriver(HiveConf hiveConf) {
    this(new QueryState.Builder().withGenerateNewQueryId(true).withHiveConf(hiveConf).build(), null,
            null);
  }

  public RemoteProcessDriver(QueryState queryState, String userName, QueryInfo queryInfo) {
    this.hiveConf = queryState.getConf();
    this.queryState = queryState;
    this.userName = userName;
    this.queryInfo = queryInfo;
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
    try {
      return getRemoteProcessClient().execute(command);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean getResults(List res) throws IOException {
    return getRemoteProcessClient().getResults(res);
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
    return new Schema();
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

  /**
   * Don't support getting the {@link Context} because it requires serializing the entire context
   * object. This method is mostly used for the {@link org.apache.hadoop.hive.ql.reexec.ReExecDriver}
   * and various unit tests.
   */
  @Override
  public Context getContext() {
    throw new UnsupportedOperationException(
            "RemoteProcessDriver does not support getting the Semantic Analyzer Context");
  }

  @Override
  public boolean hasResultSet() {
    return false;
  }

  /**
   * Get or create the {@link RemoteProcessClient} associated with this session.
   */
  private RemoteProcessClient getRemoteProcessClient() throws IOException {
    return SessionState.get().getRemoteProcessClient();
  }
}
