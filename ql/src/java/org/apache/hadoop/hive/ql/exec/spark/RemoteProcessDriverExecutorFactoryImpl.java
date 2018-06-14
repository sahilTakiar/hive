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
    private final SessionState ss; // TODO this might be problematic because all these methods
    // aren't guaranteed to run in the same thread, might need to find a way to ensure that they
    // are run in the same thread
    // Need to fix this, otherwise this won't work, SessionState isn't thread-safe, a lot of its
    // parameters are not volatile so they will get cached

    private RemoteProcessDriverExecutorImpl(String command, byte[] hiveConfBytes, String queryId) {
      // TODO fix this - properites should be inherited
      // TODO don't think this will work with an embedded HMS, although not sure
      // TODO this needs to be fixed, otherwise fetching resulsts from actual queries won't work
      // issue is that this can't be the same as tmp because the there would be two derby
      // connections which isn't allowed
      System.setProperty("test.tmp.dir",
              "/Users/stakiar/Documents/idea/apache-hive/itests/hive-unit/target/tmp");
      System.setProperty("test.tmp.dir.uri",
              "file:///Users/stakiar/Documents/idea/apache-hive/itests/hive-unit/target/tmp");
      this.hiveConf = KryoSerializer.deserializeHiveConf(hiveConfBytes);
      LOG.info("AFTER SERIALIZATION " + hiveConf);

       // TODO find a better way /place to do this, shouldn't be in the
      // RemoteProcessDriverExecutorImpl
    // because a new one is created for each Driver, but the SessionState should only be created
    // once per RemoteDriver - could put it in a static place

      if (SessionState.get() == null) {
        ss = new SessionState(hiveConf);
        SessionState.start(ss);
        LOG.info("STARTED NEW SESSION STATE " + SessionState.get());
      } else {
        ss = SessionState.get();
        LOG.info("USING EXISTING SESSION STATE " + SessionState.get());
      }
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
      SessionState.setCurrentSessionState(ss);
      return this.driver.run(command);
    }

    @Override
    public boolean getResults(List res) throws IOException {
      SessionState.setCurrentSessionState(ss);
      boolean result = this.driver.getResults(res);
      LOG.info("RETURNING RESULTS " + Arrays.toString(res.toArray()));
      return result;
    }

    @Override
    public Exception run() {
      SessionState.setCurrentSessionState(ss);
      return this.driver.run();
    }

    @Override
    public Exception compileAndRespond(String command) {
      SessionState.setCurrentSessionState(ss);
      return this.driver.compileAndRespond(command);
    }

    @Override
    public boolean hasResultSet() {
      SessionState.setCurrentSessionState(ss);
      return this.driver.hasResultSet();
    }

    @Override
    public byte[] getSchema() {
      SessionState.setCurrentSessionState(ss);
      return KryoSerializer.serialize(this.driver.getSchema());
    }

    @Override
    public boolean isFetchingTable() {
      SessionState.setCurrentSessionState(ss);
      return this.driver.isFetchingTable();
    }

    @Override
    public void close() {
      SessionState.setCurrentSessionState(ss);
      this.driver.close();
    }

    @Override
    public void destroy() {
      SessionState.setCurrentSessionState(ss);
      this.driver.destroy();
    }
  }
}
