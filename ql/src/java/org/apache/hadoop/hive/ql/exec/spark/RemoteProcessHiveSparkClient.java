package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface RemoteProcessHiveSparkClient extends Serializable {

  CommandProcessorResponse run(String command, byte[] hiveConfBytes);

  boolean getResults(List res) throws IOException;
}
