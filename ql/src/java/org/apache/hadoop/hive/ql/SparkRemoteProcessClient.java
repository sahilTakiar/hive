package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.spark.HiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.KryoSerializer;
import org.apache.hadoop.hive.ql.exec.spark.RemoteProcessHiveSparkClient;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.IOException;
import java.util.List;

public class SparkRemoteProcessClient implements RemoteProcessClient {

  private final String queryId;
  private final HiveConf hiveConf;
  private final RemoteProcessHiveSparkClient hiveSparkClient;
  private PerfLogger perfLogger;

  SparkRemoteProcessClient(String queryId, HiveConf hiveConf,
                           RemoteProcessHiveSparkClient hiveSparkClient) {
    this.queryId = queryId;
    this.hiveConf = hiveConf;
    this.hiveSparkClient = hiveSparkClient;
    this.perfLogger = SessionState.getPerfLogger();
  }

  @Override
  public CommandProcessorResponse run(String statement) {
    this.perfLogger.PerfLogBegin(getClass().getSimpleName(), "serializeHiveConf");
    byte[] hiveConfBytes = KryoSerializer.serializeHiveConf(hiveConf);
    this.perfLogger.PerfLogEnd(getClass().getSimpleName(), "serializeHiveConf");

    return this.hiveSparkClient.run(statement, hiveConfBytes);
  }

  @Override
  public boolean getResults(List res) throws IOException {
    return this.hiveSparkClient.getResults(res);
  }
}
