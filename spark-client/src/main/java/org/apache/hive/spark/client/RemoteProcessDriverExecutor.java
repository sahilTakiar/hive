package org.apache.hive.spark.client;

import java.io.IOException;
import java.util.List;

public interface RemoteProcessDriverExecutor {

  Exception run(String command);

  boolean getResults(List res) throws IOException;

  Exception run();

  Exception compileAndRespond(String command);

  boolean hasResultSet();

  byte[] getSchema();

  boolean isFetchingTable();
}
