package org.apache.hadoop.hive.ql;

public interface ContainerServiceClient {

  void execute(String statement) throws Exception;
}
