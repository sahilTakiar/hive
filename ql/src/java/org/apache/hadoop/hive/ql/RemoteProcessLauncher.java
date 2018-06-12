package org.apache.hadoop.hive.ql;

import java.io.IOException;

public interface RemoteProcessLauncher {

  RemoteProcessClient launch() throws IOException;
}
