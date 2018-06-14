package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import java.io.IOException;
import java.util.List;

interface RemoteProcessClient {

  CommandProcessorResponse run(String statement) throws Exception;

  boolean getResults(List res) throws IOException;

  CommandProcessorResponse compileAndRespond(String statement);

  CommandProcessorResponse run();

  boolean hasResultSet();

  Schema getSchema();

  boolean isFetchingTable();

  void close();

  void destroy();
}
