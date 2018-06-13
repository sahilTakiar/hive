package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.io.IOException;

public interface RemoteProcessLauncher {

  void launch() throws IOException, HiveException;
}
