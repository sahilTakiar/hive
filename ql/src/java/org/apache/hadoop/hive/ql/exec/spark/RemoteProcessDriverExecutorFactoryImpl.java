package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.spark.client.RemoteProcessDriverExecutor;
import org.apache.hive.spark.client.RemoteProcessDriverExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class RemoteProcessDriverExecutorFactoryImpl implements RemoteProcessDriverExecutorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteProcessDriverExecutorFactoryImpl.class);

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
      // TODO this needs to be fixed, otherwise fetching resulsts from actual queries won't work
      // issue is that this can't be the same as tmp because the there would be two derby
      // connections which isn't allowed
      System.setProperty("test.tmp.dir",
              "/Users/stakiar/Documents/idea/apache-hive/itests/qtest-spark/target/tmp2");
      System.setProperty("test.tmp.dir.uri",
              "file:///Users/stakiar/Documents/idea/apache-hive/itests/qtest-spark/target/tmp2");
      this.hiveConf = KryoSerializer.deserializeHiveConf(hiveConfBytes);
      LOG.info("AFTER SERIALIZATION " + hiveConf);
      SessionState ss = new SessionState(hiveConf);
      SessionState.start(ss);
      try {
        FileSystem fs = FileSystem.get(hiveConf);
        fs.listStatus(new Path("/"));
        LOG.info("CONNECTED TO " + fs.getUri());
        LOG.info("VALUE OF fs.defaultFS " + hiveConf.get("fs.defaultFS"));
      } catch (IOException e) {
        throw new RuntimeException();
      }
      this.driver = new Driver(this.hiveConf);
    }

    @Override
    public Exception run(String command) {
      LOG.info("RUNNING COMMAND " + command);
      return this.driver.run(command);
    }

    @Override
    public boolean getResults(List res) throws IOException {
      boolean result = this.driver.getResults(res);
      LOG.info("RETURNING RESULTS " + Arrays.toString(res.toArray()));
      return result;
    }

    @Override
    public Exception run() {
      return this.driver.run();
    }

    @Override
    public Exception compileAndRespond(String command) {
      return this.driver.compileAndRespond(command);
    }
  }
}
