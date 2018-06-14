package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.exec.spark.KryoSerializer;
import org.apache.hadoop.hive.ql.exec.spark.RemoteProcessHiveSparkClient;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class SparkRemoteProcessClient implements RemoteProcessClient {

  private static final Logger LOG = LoggerFactory.getLogger(SparkRemoteProcessClient.class);

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

  @Override
  public CommandProcessorResponse compileAndRespond(String statement) {
    this.perfLogger.PerfLogBegin(getClass().getSimpleName(), "serializeHiveConf");
    byte[] hiveConfBytes = KryoSerializer.serializeHiveConf(hiveConf);
    this.perfLogger.PerfLogEnd(getClass().getSimpleName(), "serializeHiveConf");
    return this.hiveSparkClient.compileAndRespond(statement, hiveConfBytes);
  }

  @Override
  public CommandProcessorResponse run() {
    return this.hiveSparkClient.run();
  }

  @Override
  public boolean hasResultSet() {
    return this.hiveSparkClient.hasResultSet();
  }

  @Override
  public Schema getSchema() {
    return this.hiveSparkClient.getSchema();
  }

  @Override
  public boolean isFetchingTable() {
    return this.hiveSparkClient.isFetchingTable();
  }

  @Override
  public void close() {
    this.hiveSparkClient.close();
  }

  @Override
  public void destroy() {
    this.hiveSparkClient.destroy();
  }
}
