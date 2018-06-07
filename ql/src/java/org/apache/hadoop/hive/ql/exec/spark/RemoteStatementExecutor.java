package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.spark.client.JobContext;

import java.io.Serializable;

public class RemoteStatementExecutor implements Serializable {

  private final HiveConf hiveConf;
  private final JobContext jc;
  private final String statement;

  public RemoteStatementExecutor(HiveConf hiveConf, JobContext jc,
                                 String statement) {
    this.hiveConf = hiveConf;
    this.jc = jc;
    this.statement = statement;
  }

  public Serializable run() {
    DriverFactory.newDriver(this.hiveConf).run(statement);
    return null;
  }
}
