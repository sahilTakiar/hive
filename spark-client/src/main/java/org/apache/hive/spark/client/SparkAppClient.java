package org.apache.hive.spark.client;

import java.io.Serializable;
import java.net.URI;
import java.util.concurrent.Future;

public interface SparkAppClient {

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
   * Stops the remote context.
   *
   * Any pending jobs will be cancelled, and the remote context will be torn down.
   */
  void stop();

  /**
   * Adds a jar file to the running remote context.
   *
   * Note that the URL should be reachable by the Spark driver process. If running the driver
   * in cluster mode, it may reside on a different host, meaning "file:" URLs have to exist
   * on that node (and not on the client machine).
   *
   * @param uri The location of the jar file.
   * @return A future that can be used to monitor the operation.
   */
  Future<?> addJar(URI uri);

  /**
   * Adds a file to the running remote context.
   *
   * Note that the URL should be reachable by the Spark driver process. If running the driver
   * in cluster mode, it may reside on a different host, meaning "file:" URLs have to exist
   * on that node (and not on the client machine).
   *
   * @param uri The location of the file.
   * @return A future that can be used to monitor the operation.
   */
  Future<?> addFile(URI uri);

  /**
   * Get the count of executors.
   */
  Future<Integer> getExecutorCount();

  /**
   * Get default parallelism. For standalone mode, this can be used to get total number of cores.
   */
  Future<Integer> getDefaultParallelism();

  /**
   * Check if remote context is still active.
   */
  boolean isActive();

  /**
   * Cancel the specified jobId
   *
   * @param jobId the jobId to cancel
   */
  void cancel(String jobId);
}
