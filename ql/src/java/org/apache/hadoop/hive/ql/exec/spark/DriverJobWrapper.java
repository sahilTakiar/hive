package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hive.spark.client.BaseProtocol;
import org.apache.hive.spark.client.RemoteDriver;
import org.apache.hive.spark.client.Submittable;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class DriverJobWrapper<T extends Serializable> implements Callable<Void>, Submittable {

  private RemoteDriver remoteDriver;
  private BaseProtocol.ExecuteStatement req;
  private Future<Void> future;

  public DriverJobWrapper(RemoteDriver remoteDriver, BaseProtocol.ExecuteStatement req) {
    this.remoteDriver = remoteDriver;
    this.req = req;
  }

  @Override
  public Void call() throws Exception {
    //req.job.call(remoteDriver.jc);
    // TODO still not sure how to do this - for a HACK make the DriverProtocol just contain a
    // Driver instance and just run it as it get requets for certain things - maybe is
    // synchronous for now - will need to move the Client and Driver Protocol into ql
    return null;
  }

  @Override
  public void submit() {
    this.future = remoteDriver.executor.submit(this);
  }
}
