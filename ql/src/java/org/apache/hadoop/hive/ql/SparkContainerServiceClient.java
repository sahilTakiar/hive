package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;

public class SparkContainerServiceClient implements ContainerServiceClient {

  private final SparkSession sparkSession;

  SparkContainerServiceClient(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  @Override
  public void execute(String statement) throws Exception {
    this.sparkSession.submit(statement);
  }
}
