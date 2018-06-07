package org.apache.hive.service.cli.operation;

import org.apache.hadoop.hive.ql.ContainerDriver;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;

import java.io.IOException;
import java.util.Map;


/**
 * Runs a SQL query in a remote process.
 */
public class RemoteSQLOperation extends ExecuteStatementOperation {

  private final ContainerDriver containerDriver;

  public RemoteSQLOperation(HiveSession parentSession, String statement,
                            Map<String, String> confOverlay, boolean runAsync,
                            long queryTimeout) throws HiveSQLException {
    super(parentSession, statement, confOverlay, runAsync);
    this.containerDriver = new ContainerDriver(parentSession.getSessionConf());
  }

  @Override
  protected void runInternal() throws HiveSQLException {
    containerDriver.run(statement);
  }

  @Override
  public void cancel(OperationState stateAfterCancel) throws HiveSQLException {

  }

  @Override
  public void close() throws HiveSQLException {

  }

  @Override
  public TableSchema getResultSetSchema() throws HiveSQLException {
    return null;
  }

  @Override
  public RowSet getNextRowSet(FetchOrientation orientation, long maxRows) throws HiveSQLException {
    return null;
  }
}
