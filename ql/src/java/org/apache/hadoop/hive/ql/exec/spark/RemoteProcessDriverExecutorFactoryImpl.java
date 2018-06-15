package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.spark.client.RemoteProcessDriverExecutor;
import org.apache.hive.spark.client.RemoteProcessDriverExecutorFactory;

import java.io.IOException;
import java.util.List;

public class RemoteProcessDriverExecutorFactoryImpl implements RemoteProcessDriverExecutorFactory {

  private final HiveConf hiveConf;

  public RemoteProcessDriverExecutorFactoryImpl(byte[] hiveConfBytes) {
    this.hiveConf = KryoSerializer.deserializeHiveConf(hiveConfBytes);
    SessionState.start(this.hiveConf);
  }

  @Override
  public RemoteProcessDriverExecutor createRemoteProcessDriverExecutor(byte[] hiveConfBytes) {
    return new RemoteProcessDriverExecutorImpl(hiveConfBytes);
  }

  private static final class RemoteProcessDriverExecutorImpl implements RemoteProcessDriverExecutor {

    private HiveConf hiveConf;
    private IDriver driver;

    private RemoteProcessDriverExecutorImpl(byte[] hiveConfBytes) {
      this.hiveConf = KryoSerializer.deserializeHiveConf(hiveConfBytes);
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

    @Override
    public Exception run() {
      return this.driver.run();
    }

    @Override
    public Exception compileAndRespond(String command) {
      return this.driver.compileAndRespond(command);
    }

    @Override
    public boolean hasResultSet() {
      return this.driver.hasResultSet();
    }

    @Override
    public byte[] getSchema() {
      return KryoSerializer.serialize(this.driver.getSchema());
    }

    @Override
    public boolean isFetchingTable() {
      return this.driver.isFetchingTable();
    }

    @Override
    public void close() {
      this.driver.close();
    }

    @Override
    public void destroy() {
      this.driver.destroy();
    }
  }
}
