package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.spark.client.QueryExecutorService;

import java.io.IOException;
import java.util.List;

public class RemoteDriverQueryExecutorService implements QueryExecutorService {

  private HiveConf hiveConf;
  private IDriver driver;

  @Override
  public void run(String command, byte[] hiveConfBytes) {
    System.setProperty("test.tmp.dir" ,
            "/Users/stakiar/Documents/idea/apache-hive/itests/qtest-spark/target/tmp2");
    System.setProperty("test.tmp.dir.uri" ,
            "file:///Users/stakiar/Documents/idea/apache-hive/itests/qtest-spark/target/tmp2");
    this.hiveConf = KryoSerializer.deserializeHiveConf(hiveConfBytes);
    SessionState ss = new SessionState(hiveConf);
    SessionState.start(ss);
    this.driver = new Driver(this.hiveConf);
    this.driver.run(command);
  }

  @Override
  public boolean getResults(List res) throws IOException {
    return this.driver.getResults(res);
  }
}
