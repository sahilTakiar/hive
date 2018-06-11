package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hive.spark.client.QueryExecutorService;

import java.io.IOException;
import java.util.List;

public class RemoteDriverQueryExecutorService implements QueryExecutorService {

  private final HiveConf hiveConf;
  private final IDriver driver;

  RemoteDriverQueryExecutorService(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    this.driver = DriverFactory.newDriver(this.hiveConf);
  }

  @Override
  public void run(String command) {
    this.driver.run(command);
  }

  @Override
  public boolean getResults(List res) throws IOException {
    return this.driver.getResults(res);
  }
}
