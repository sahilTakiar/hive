package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hive.spark.client.AbstractSparkClient;

import java.util.List;

public class RemoteProcessHiveSparkClientImpl implements RemoteProcessHiveSparkClient {


  private final String queryId;
  private final AbstractSparkClient.ClientProtocol clientProtocol;

  public RemoteProcessHiveSparkClientImpl(String queryId,
                                          AbstractSparkClient.ClientProtocol clientProtocol) {
    this.queryId = queryId;
    this.clientProtocol = clientProtocol;
  }

  @Override
  public CommandProcessorResponse run(String command, byte[] hiveConfBytes) {
    return (CommandProcessorResponse) this.clientProtocol.run(command, hiveConfBytes, this.queryId);
  }

  @Override
  public boolean getResults(List res) {
    return this.clientProtocol.getResults(this.queryId, res);
  }

  @Override
  public CommandProcessorResponse compileAndRespond(String statement, byte[] hiveConfBytes) {
    return (CommandProcessorResponse) this.clientProtocol.compileAndRespond(this.queryId, statement,
            hiveConfBytes);
  }

  @Override
  public CommandProcessorResponse run() {
    return (CommandProcessorResponse) this.clientProtocol.run(this.queryId);
  }

  @Override
  public boolean hasResultSet() {
    return this.clientProtocol.hasResultSet(this.queryId);
  }

  @Override
  public Schema getSchema() {
    return KryoSerializer.deserialize(this.clientProtocol.getSchema(this.queryId), Schema.class);
  }

  @Override
  public boolean isFetchingTable() {
    return this.clientProtocol.isFetchingTable(this.queryId);
  }

  @Override
  public void close() {
    this.clientProtocol.closeDriver(this.queryId);
  }

  @Override
  public void destroy() {
    this.clientProtocol.destroyDriver(this.queryId);
  }

  @Override
  public QueryDisplay getQueryDisplay() {
    return KryoSerializer.deserialize(this.clientProtocol.getQueryDisplay(this.queryId),
            QueryDisplay.class);
  }
}
