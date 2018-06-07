package org.apache.hadoop.hive.ql.exec.spark.session;

import org.apache.hadoop.hive.ql.DriverContext;
import org.apache.hadoop.hive.ql.exec.spark.status.SparkJobRef;
import org.apache.hadoop.hive.ql.plan.SparkWork;

public interface SparkWorkSubmitter {

  SparkJobRef submit(DriverContext driverContext, SparkWork sparkWork) throws Exception;
}
