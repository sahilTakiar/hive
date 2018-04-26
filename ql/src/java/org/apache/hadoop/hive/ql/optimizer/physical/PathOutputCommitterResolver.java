/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.physical;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;

import org.apache.hadoop.hive.common.BlobStorageUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PathOutputCommitterSetupTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.PathOutputCommitterWork;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Collectors;


public class PathOutputCommitterResolver implements PhysicalPlanResolver {

  private static final Logger LOG = LoggerFactory.getLogger(PathOutputCommitterResolver.class);

  // Useful for testing
  private static final String HIVE_BLOBSTORE_COMMIT_DISABLE_EXPLAIN = "hive.blobstore.commit." +
          "disable.explain";

  private final Map<Task<?>, Collection<FileSinkOperator>> taskToFsOps = new HashMap<>();
  private final List<Task<MoveWork>> mvTasks = new ArrayList<>();
  private HiveConf hconf;

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    this.hconf = pctx.getConf();

    // Collect all MoveTasks and FSOPs
    TaskGraphWalker graphWalker = new TaskGraphWalker(new S3ACommitDispatcher());
    List<Node> rootTasks = new ArrayList<>(pctx.getRootTasks());
    graphWalker.startWalking(rootTasks, null);

    // Find MoveTasks with no child MoveTask
    List<Task<MoveWork>> sinkMoveTasks = mvTasks.stream()
            .filter(mvTask -> !containsChildTask(mvTask.getChildTasks(), MoveTask.class))
            .collect(Collectors.toList());

    // Iterate through each FSOP
    for (Map.Entry<Task<?>, Collection<FileSinkOperator>> entry : taskToFsOps.entrySet()) {
      for (FileSinkOperator fsOp : entry.getValue()) {
        try {
          processFsOp(entry.getKey(), fsOp, sinkMoveTasks);
        } catch (HiveException | MetaException e) {
          throw new SemanticException(e);
        }
      }
    }

    return pctx;
  }

  private boolean containsChildTask(List<Task<? extends Serializable>> mvTasks, Class<MoveTask>
          taskClass) {
    if (mvTasks == null) {
      return false;
    }
    boolean containsChildTask = false;
    for (Task<? extends Serializable> mvTask : mvTasks) {
      if (taskClass.isInstance(mvTask)) {
        return true;
      }
      containsChildTask = containsChildTask(mvTask.getChildTasks(), taskClass);
    }
    return containsChildTask;
  }

  private class S3ACommitDispatcher implements Dispatcher {

    @Override
    public Object dispatch(Node nd, Stack<Node> stack,
                           Object... nodeOutputs) throws SemanticException {

      Task<?> task = (Task<?>) nd;
      Collection<FileSinkOperator> fsOps = getAllFsOps(task);
      if (!fsOps.isEmpty()) {
        taskToFsOps.put((Task<?>) nd, fsOps);
      }
      if (nd instanceof MoveTask) {
        mvTasks.add((MoveTask) nd);
      }
      return null;
    }
  }

  private Collection<FileSinkOperator> getAllFsOps(Task<?> task) {
    Collection<Operator<?>> fsOps = new ArrayList<>();
    if (task instanceof MapRedTask) {
      fsOps.addAll(((MapRedTask) task).getWork().getAllOperators());
    } else if (task instanceof SparkTask) {
      for (BaseWork work : ((SparkTask) task).getWork().getAllWork()) {
        fsOps.addAll(work.getAllOperators());
      }
    }
    return fsOps.stream()
            .filter(FileSinkOperator.class::isInstance)
            .map(FileSinkOperator.class::cast)
            .collect(Collectors.toList());
  }

  private void processFsOp(Task<?> task, FileSinkOperator fsOp,
                           List<Task<MoveWork>> sinkMoveTasks) throws HiveException, MetaException {
    FileSinkDesc fileSinkDesc = fsOp.getConf();

    // Get the MoveTask that will process the output of the fsOp
    Task<MoveWork> mvTask = GenMapRedUtils.findMoveTaskForFsopOutput(sinkMoveTasks,
            fileSinkDesc.getFinalDirName(), fileSinkDesc.isMmTable());

    if (mvTask != null) {

      MoveWork mvWork = mvTask.getWork();

      // Don't process the mvTask if it requires committing data for DP queries
      if (mvWork.getLoadMultiFilesWork() != null && (mvWork.getLoadTableWork() == null || mvWork
              .getLoadTableWork().getDPCtx() == null)) {

        // The final output path we will commit data to
        Path outputPath = null;

        // Instead of picking between load table work and load file work, throw an exception if
        // they are both set (this should never happen)
        if (mvWork.getLoadTableWork() != null && mvWork.getLoadFileWork() != null) {
          throw new IllegalArgumentException("Load Table Work and Load File Work cannot both be " +
                  "set");
        }

        // If there is a load file work, get its output path
        if (mvWork.getLoadFileWork() != null) {
          outputPath = getLoadFileOutputPath(mvWork);
        }

        // If there is a load table work, get is output path
        if (mvTask.getWork().getLoadTableWork() != null) {
          outputPath = getLoadTableOutputPath(mvWork);
        }
        if (outputPath != null) {
          if (BlobStorageUtils.isBlobStoragePath(hconf, outputPath)) {

            if ("s3a".equals(outputPath.toUri().getScheme())) {
              setupS3aOutputCommitter(mvWork);
            }

            PathOutputCommitterWork setupWork = createPathOutputCommitterWork(outputPath);
            mvWork.setPathOutputCommitterWork(setupWork);

            fileSinkDesc.setHasOutputCommitter(true);
            fileSinkDesc.setTargetDirName(outputPath.toString());

            LOG.info("Using Output Committer " + setupWork.getPathOutputCommitterClass() +
                    " for MoveTask: " + mvTask + ", FileSinkOperator: " + fsOp + " and output " +
                    "path: " + outputPath);

            if (hconf.getBoolean(HIVE_BLOBSTORE_COMMIT_DISABLE_EXPLAIN, false)) {
              PathOutputCommitterSetupTask setupTask = new PathOutputCommitterSetupTask();
              setupTask.setWork(setupWork);
              setupTask.executeTask(null);
            } else {
              task.addDependentTask(TaskFactory.get(setupWork));
            }
          }
        }
      }
    }
  }

  private PathOutputCommitterWork createPathOutputCommitterWork(Path outputPath) {
    JobID jobID = new JobID();
    TaskAttemptContext taskAttemptContext = createTaskAttemptContext(jobID);
    JobContext jobContext = new JobContextImpl(hconf, jobID);

    return new PathOutputCommitterWork(outputPath.toString(),
            jobContext, taskAttemptContext);
  }

  private void setupS3aOutputCommitter(MoveWork mvWork) {
    if (mvWork.getLoadTableWork() != null && mvWork.getLoadTableWork()
            .isInsertOverwrite()) {
      hconf.set(CommitConstants.FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CommitConstants.
              CONFLICT_MODE_REPLACE);
    } else {
      hconf.set(CommitConstants.FS_S3A_COMMITTER_STAGING_CONFLICT_MODE, CommitConstants
              .CONFLICT_MODE_APPEND);
    }

    // We set this to false because its better for Hive to have more control over the file
    // names given the that the JobID we are creating has no meaning
    hconf.setBoolean(CommitConstants.FS_S3A_COMMITTER_STAGING_UNIQUE_FILENAMES, false);
  }

  // Not sure if this is the best way to do this, somewhat copied from TaskCompiler, which uses
  // similar logic to get the default location for the target table in a CTAS query
  // don't think will work if a custom location is specified
  // works because you can't specify a custom location when auto-creating a partition
  private Path getDefaultPartitionPath(Path tablePath, Map<String, String> partitionSpec)
          throws MetaException {
    Warehouse wh = new Warehouse(hconf);
    return wh.getPartitionPath(tablePath, partitionSpec);
  }

  private TaskAttemptContext createTaskAttemptContext(JobID jobID) {
    return new TaskAttemptContextImpl(hconf,
            new TaskAttemptID(jobID.getJtIdentifier(), jobID.getId(), TaskType.JOB_SETUP, 0, 0));
  }

  private Path getLoadFileOutputPath(MoveWork mvWork) {
    return mvWork.getLoadFileWork().getTargetDir();
  }

  private Path getLoadTableOutputPath(MoveWork mvWork) throws HiveException, MetaException {
    if (mvWork.getLoadTableWork().getPartitionSpec() != null &&
            !mvWork.getLoadTableWork().getPartitionSpec().isEmpty()) {
      return getLoadPartitionOutputPath(mvWork);
    } else {
      // should probably replace this with Hive.getTable().getDataLocation()
      return new Path(mvWork.getLoadTableWork().getTable().getProperties()
              .getProperty("location")); // INSERT INTO ... VALUES (...)
    }
  }

  private Path getLoadPartitionOutputPath(MoveWork mvWork) throws HiveException, MetaException {
    Hive db = Hive.get();
    Partition partition = db.getPartition(db.getTable(mvWork.getLoadTableWork()
                    .getTable().getTableName()),
            mvWork.getLoadTableWork().getPartitionSpec(), false);
    if (partition != null) {
      return partition.getDataLocation();
    } else {
      return getDefaultPartitionPath(db.getTable(mvWork.getLoadTableWork()
              .getTable().getTableName()).getDataLocation(), mvWork
              .getLoadTableWork().getPartitionSpec());
    }
  }
}
