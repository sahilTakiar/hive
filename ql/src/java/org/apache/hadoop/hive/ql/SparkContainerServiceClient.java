package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import java.io.IOException;
import java.util.List;

public class SparkContainerServiceClient implements ContainerServiceClient {

  private final SparkSession sparkSession;

  SparkContainerServiceClient(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @Override
  public CommandProcessorResponse execute(String statement) throws Exception {
    return this.sparkSession.submit(statement);
  }

  @Override
  public boolean getResults(List res) throws IOException {
    return this.sparkSession.getResults(res);
  }
}
