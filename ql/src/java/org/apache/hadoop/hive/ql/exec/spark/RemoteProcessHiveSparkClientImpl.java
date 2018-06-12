package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hive.spark.client.ClientProtocol;

import java.io.IOException;
import java.util.List;

public class RemoteProcessHiveSparkClientImpl implements RemoteProcessHiveSparkClient {

  private final ClientProtocol clientProtocol;

  public RemoteProcessHiveSparkClientImpl(ClientProtocol clientProtocol) {
    this.clientProtocol = clientProtocol;
  }

  // TODO it should really just be the conf overlay rather than the whole hiveconf
  @Override
  public CommandProcessorResponse run(String command, byte[] hiveConfBytes) {
    this.clientProtocol.runStatement(command, hiveConfBytes);
    return new CommandProcessorResponse(0);
  }

  @Override
  public boolean getResults(List res) throws IOException {
    this.clientProtocol.sendGetResulsts();
    try {
      synchronized (this.clientProtocol.resultsLock) {
        this.clientProtocol.resultsLock.wait();
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return this.clientProtocol.getResults(res);
  }
}
