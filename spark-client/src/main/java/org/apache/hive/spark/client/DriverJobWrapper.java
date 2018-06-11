package org.apache.hive.spark.client;

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
    req.job.call(remoteDriver.jc);
    return null;
  }

  @Override
  public void submit() {
    this.future = remoteDriver.executor.submit(this);
  }
}
