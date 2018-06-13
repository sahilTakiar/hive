package org.apache.hive.spark.client;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Future;

public interface JobSparkClient extends Serializable {

  /**
   * Submits a job for asynchronous execution.
   *
   * @param job The job to execute.
   * @return A handle that be used to monitor the job.
   */
  <T extends Serializable> JobHandle<T> submit(Job<T> job);

  /**
   * Submits a job for asynchronous execution.
   *
   * @param job The job to execute.
   * @param listeners jobhandle listeners to invoke during the job processing
   * @return A handle that be used to monitor the job.
   */
  <T extends Serializable> JobHandle<T> submit(Job<T> job, List<JobHandle.Listener<T>> listeners);

  /**
   * Asks the remote context to run a job immediately.
   * <p>
   * Normally, the remote context will queue jobs and execute them based on how many worker
   * threads have been configured. This method will run the submitted job in the same thread
   * processing the RPC message, so that queueing does not apply.
   * </p>
   * <p>
   * It's recommended that this method only be used to run code that finishes quickly. This
   * avoids interfering with the normal operation of the context.
   * </p>
   * Note: the JobContext#monitor() functionality is not available when using this method.
   *
   * @param job The job to execute.
   * @return A future to monitor the result of the job.
   */
  <T extends Serializable> Future<T> run(Job<T> job);

  /**
   * Cancel the specified jobId
   *
   * @param jobId the jobId to cancel
   */
  void cancel(String jobId);

  /**
   * Check if remote context is still active.
   */
  boolean isActive();
}
