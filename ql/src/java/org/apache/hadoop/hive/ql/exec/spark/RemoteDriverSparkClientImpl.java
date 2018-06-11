package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hive.spark.client.ClientProtocol;

import java.io.IOException;
import java.util.List;

public class RemoteDriverSparkClientImpl implements RemoteDriverSparkClient {

  private final ClientProtocol clientProtocol;

  RemoteDriverSparkClientImpl(ClientProtocol clientProtocol) {
    this.clientProtocol = clientProtocol;
  }

  @Override
  public CommandProcessorResponse run(String command) {
    return null;
  }

  @Override
  public boolean getResults(List res) throws IOException {
    this.clientProtocol.sendGetResulsts();
    try {
      this.clientProtocol.resultsLock.wait();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    this.clientProtocol.getResults(res);
    return true;
  }
}
