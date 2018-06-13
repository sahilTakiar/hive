package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hive.spark.client.ClientProtocol;

import java.io.IOException;
import java.util.List;

public class RemoteProcessHiveSparkClientImpl implements RemoteProcessHiveSparkClient {

  private final String queryId;
  private final ClientProtocol clientProtocol;

  public RemoteProcessHiveSparkClientImpl(String queryId, ClientProtocol clientProtocol) {
    this.queryId = queryId;
    this.clientProtocol = clientProtocol;
  }

  @Override
  public CommandProcessorResponse run(String command, byte[] hiveConfBytes) {
    this.clientProtocol.run(command, hiveConfBytes, this.queryId);
    return new CommandProcessorResponse(0);
  }

  @Override
  public boolean getResults(List res) throws IOException {
    return this.clientProtocol.getResults(null, res);
  }
}
