package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hive.spark.client.ClientProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class RemoteProcessHiveSparkClientImpl implements RemoteProcessHiveSparkClient {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteProcessHiveSparkClientImpl.class);

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
    boolean result = this.clientProtocol.getResults(this.queryId, res);
    LOG.info("GOT RESULTS " + res);
    return result;
  }

  @Override
  public CommandProcessorResponse compileAndRespond(String statement, byte[] hiveConfBytes) {
    this.clientProtocol.compileAndRespond(this.queryId, statement, hiveConfBytes);
    return new CommandProcessorResponse(0);
  }

  @Override
  public CommandProcessorResponse run() {
    this.clientProtocol.run(this.queryId);
    return new CommandProcessorResponse(0);
  }
}
