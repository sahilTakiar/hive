package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.spark.client.RemoteProcessDriverExecutor;
import org.apache.hive.spark.client.RemoteProcessDriverExecutorFactory;
import org.apache.hive.spark.client.rpc.Rpc;

import java.io.IOException;
import java.util.List;

public class RemoteProcessDriverExecutorFactoryImpl implements RemoteProcessDriverExecutorFactory {

  @Override
  public RemoteProcessDriverExecutor createRemoteProcessDriverExecutor(String command,
                                                                       byte[] hiveConfBytes,
                                                                       String queryId) {
    return new RemoteProcessDriverExecutorImpl(command, hiveConfBytes, queryId);
  }

  private static final class RemoteProcessDriverExecutorImpl implements RemoteProcessDriverExecutor {

    private HiveConf hiveConf;
    private IDriver driver;

    private RemoteProcessDriverExecutorImpl(String command, byte[] hiveConfBytes, String queryId) {
      // TODO fix this - properites should be inherited
      // TODO don't think this will work with an embedded HMS, although not sure
      System.setProperty("test.tmp.dir",
              "/Users/stakiar/Documents/idea/apache-hive/itests/qtest-spark/target/tmp2");
      System.setProperty("test.tmp.dir.uri",
              "file:///Users/stakiar/Documents/idea/apache-hive/itests/qtest-spark/target/tmp2");
      this.hiveConf = KryoSerializer.deserializeHiveConf(hiveConfBytes);
      SessionState ss = new SessionState(hiveConf);
      SessionState.start(ss);
      this.driver = new Driver(this.hiveConf);
    }

    @Override
    public Exception run(String command) {
      return this.driver.run(command);
    }

    @Override
    public boolean getResults(List res) throws IOException {
      return this.driver.getResults(res);
    }
  }
}
