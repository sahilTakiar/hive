package org.apache.hadoop.hive.ql.exec.spark.session;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.spark.LocalHiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.RemoteHiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.exec.spark.status.impl.JobMetricsListener;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hive.spark.client.BaseProtocol;
import org.apache.hive.spark.client.Job;
import org.apache.hive.spark.client.JobHandle;
import org.apache.hive.spark.client.JobWrapper;
import org.apache.hive.spark.client.RemoteDriver;
import org.apache.hive.spark.client.SparkClient;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

public class SparkLocalClientWorkSubmitter implements SparkWorkSubmitter {

  private final HiveConf hiveConf;
  private final RemoteDriver remoteDriver;

  public SparkLocalClientWorkSubmitter(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    this.remoteDriver = RemoteDriver.getInstance();
  }

  @Override
  public SparkJobRef submit(DriverContext driverContext, SparkWork sparkWork) throws Exception {
    // TODO use the LocalHiveSparkClient to submit the SparkWork
    // TODO pass the JavaSparkContext through the DriverContext?
    JobMetricsListener jobMetricsListener = new JobMetricsListener();
    RemoteDriver.getInstance().sc().sc().addSparkListener(jobMetricsListener);
    return new LocalHiveSparkClient(RemoteDriver.getInstance().sc(), jobMetricsListener).execute(driverContext, sparkWork);
//    return new RemoteHiveSparkClient(this.hiveConf, new SparkClient() {
//      @Override
//      public <T extends Serializable> JobHandle<T> submit(Job<T> job) {
//        remoteDriver.submit(new JobWrapper<>(remoteDriver, new BaseProtocol
//                .JobRequest<T>(
//                UUID.randomUUID().toString(), job)));
//        return null;
//      }
//
//      @Override
//      public <T extends Serializable> JobHandle<T> submit(Job<T> job,
//                                                          List<JobHandle.Listener<T>> listeners) {
//        return null;
//      }
//
//      @Override
//      public <T extends Serializable> Future<T> run(Job<T> job) {
//        return null;
//      }
//
//      @Override
//      public void stop() {
//
//      }
//
//      @Override
//      public Future<?> addJar(URI uri) {
//        return null;
//      }
//
//      @Override
//      public Future<?> addFile(URI uri) {
//        return null;
//      }
//
//      @Override
//      public Future<Integer> getExecutorCount() {
//        return null;
//      }
//
//      @Override
//      public Future<Integer> getDefaultParallelism() {
//        return null;
//      }
//
//      @Override
//      public boolean isActive() {
//        return false;
//      }
//
//      @Override
//      public void cancel(String jobId) {
//
//      }
//    }).run(driverContext, sparkWork);
  }
}
