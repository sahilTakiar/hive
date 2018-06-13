package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import java.io.IOException;
import java.util.List;

interface RemoteProcessClient {

  CommandProcessorResponse run(String statement) throws Exception;

  boolean getResults(List res) throws IOException;
}
