package org.apache.hive.spark.client;

import java.io.IOException;
import java.util.List;

public interface QueryExecutorService {

  void run(String command);

  boolean getResults(List res) throws IOException;
}
