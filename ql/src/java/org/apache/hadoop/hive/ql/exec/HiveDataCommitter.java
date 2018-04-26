package org.apache.hadoop.hive.ql.exec;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.io.FilenameUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A {@link DataCommitter} that commits Hive data using a {@link FileSystem}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HiveDataCommitter implements DataCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(DataCommitter.class.getName());

  //it is assumed that parent directory of the destf should already exist when this
  //method is called. when the replace value is true, this method works a little different
  //from mv command if the destf is a directory, it replaces the destf instead of moving under
  //the destf. in this case, the replaced destf still preserves the original destf's permission
  @Override
  public boolean moveFile(HiveConf conf, Path srcf, Path destf, boolean replace,
                          boolean isSrcLocal) throws HiveException {
    final FileSystem srcFs, destFs;
    try {
      destFs = destf.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to get dest fs", e);
      throw new HiveException(e.getMessage(), e);
    }
    try {
      srcFs = srcf.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to get src fs", e);
      throw new HiveException(e.getMessage(), e);
    }

    HdfsUtils.HadoopFileStatus destStatus = null;

    // If source path is a subdirectory of the destination path (or the other way around):
    //   ex: INSERT OVERWRITE DIRECTORY 'target/warehouse/dest4.out' SELECT src.value WHERE src.key >= 300;
    //   where the staging directory is a subdirectory of the destination directory
    // (1) Do not delete the dest dir before doing the move operation.
    // (2) It is assumed that subdir and dir are in same encryption zone.
    // (3) Move individual files from scr dir to dest dir.
    boolean srcIsSubDirOfDest = Hive.isSubDir(srcf, destf, srcFs, destFs, isSrcLocal),
        destIsSubDirOfSrc = Hive.isSubDir(destf, srcf, destFs, srcFs, false);
    final String msg = "Unable to move source " + srcf + " to destination " + destf;
    try {
      if (replace) {
        try{
          destStatus = new HdfsUtils.HadoopFileStatus(conf, destFs, destf);
          //if destf is an existing directory:
          //if replace is true, delete followed by rename(mv) is equivalent to replace
          //if replace is false, rename (mv) actually move the src under dest dir
          //if destf is an existing file, rename is actually a replace, and do not need
          // to delete the file first
          if (replace && !srcIsSubDirOfDest) {
            destFs.delete(destf, true);
            LOG.debug("The path " + destf.toString() + " is deleted");
          }
        } catch (FileNotFoundException ignore) {
        }
      }
      final HdfsUtils.HadoopFileStatus desiredStatus = destStatus;
      final SessionState parentSession = SessionState.get();
      if (isSrcLocal) {
        // For local src file, copy to hdfs
        destFs.copyFromLocalFile(srcf, destf);
        return true;
      } else {
        if (needToCopy(srcf, destf, srcFs, destFs)) {
          //copy if across file system or encryption zones.
          LOG.debug("Copying source " + srcf + " to " + destf + " because HDFS encryption zones are different.");
          return FileUtils.copy(srcf.getFileSystem(conf), srcf, destf.getFileSystem(conf), destf,
              true,    // delete source
              replace, // overwrite destination
              conf);
        } else {
          if (srcIsSubDirOfDest || destIsSubDirOfSrc) {
            FileStatus[] srcs = destFs.listStatus(srcf, FileUtils.HIDDEN_FILES_PATH_FILTER);

            List<Future<Void>> futures = new LinkedList<>();
            final ExecutorService pool = conf.getInt(HiveConf.ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25) > 0 ?
                Executors.newFixedThreadPool(conf.getInt(HiveConf.ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Move-Thread-%d").build()) : null;
            if (destIsSubDirOfSrc && !destFs.exists(destf)) {
              if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
                Utilities.FILE_OP_LOGGER.trace("Creating " + destf);
              }
              destFs.mkdirs(destf);
            }
            /* Move files one by one because source is a subdirectory of destination */
            for (final FileStatus srcStatus : srcs) {

              final Path destFile = new Path(destf, srcStatus.getPath().getName());

              final String poolMsg =
                  "Unable to move source " + srcStatus.getPath() + " to destination " + destFile;

              if (null == pool) {
                boolean success = false;
                if (destFs instanceof DistributedFileSystem) {
                  ((DistributedFileSystem)destFs).rename(srcStatus.getPath(), destFile, Options.Rename.OVERWRITE);
                  success = true;
                } else {
                  destFs.delete(destFile, false);
                  success = destFs.rename(srcStatus.getPath(), destFile);
                }
                if(!success) {
                  throw new IOException("rename for src path: " + srcStatus.getPath() + " to dest:"
                      + destf + " returned false");
                }
              } else {
                futures.add(pool.submit(new Callable<Void>() {
                  @Override
                  public Void call() throws HiveException {
                    SessionState.setCurrentSessionState(parentSession);
                    final String group = srcStatus.getGroup();
                    try {
                      boolean success = false;
                      if (destFs instanceof DistributedFileSystem) {
                        ((DistributedFileSystem)destFs).rename(srcStatus.getPath(), destFile, Options.Rename.OVERWRITE);
                        success = true;
                      } else {
                        destFs.delete(destFile, false);
                        success = destFs.rename(srcStatus.getPath(), destFile);
                      }
                      if (!success) {
                        throw new IOException(
                            "rename for src path: " + srcStatus.getPath() + " to dest path:"
                                + destFile + " returned false");
                      }
                    } catch (Exception e) {
                      throw Hive.getHiveException(e, poolMsg);
                    }
                    return null;
                  }
                }));
              }
            }
            if (null != pool) {
              pool.shutdown();
              for (Future<Void> future : futures) {
                try {
                  future.get();
                } catch (Exception e) {
                  throw handlePoolException(pool, e);
                }
              }
            }
            return true;
          } else {
            if (destFs.rename(srcf, destf)) {
              return true;
            }
            return false;
          }
        }
      }
    } catch (Exception e) {
      throw Hive.getHiveException(e, msg);
    }
  }

  /**
   * Copy files.  This handles building the mapping for buckets and such between the source and
   * destination
   * @param conf Configuration object
   * @param srcf source directory, if bucketed should contain bucket files
   * @param destf directory to move files into
   * @param fs Filesystem
   * @param isSrcLocal true if source is on local file system
   * @param isAcidIUD true if this is an ACID based Insert/Update/Delete
   * @param isOverwrite if true, then overwrite if destination file exist, else add a duplicate copy
   * @param newFiles if this is non-null, a list of files that were created as a result of this
   *                 move will be returned.
   * @throws HiveException
   */
  @Override
  public void copyFiles(HiveConf conf, Path srcf, Path destf, FileSystem fs, boolean isSrcLocal,
                        boolean isAcidIUD, boolean isOverwrite, List<Path> newFiles,
                        boolean isBucketed, boolean isFullAcidTable) throws HiveException {
    try {
      // create the destination if it does not exist
      if (!fs.exists(destf)) {
        FileUtils.mkdir(fs, destf, conf);
      }
    } catch (IOException e) {
      throw new HiveException(
          "copyFiles: error while checking/creating destination directory!!!",
          e);
    }

    FileStatus[] srcs;
    FileSystem srcFs;
    try {
      srcFs = srcf.getFileSystem(conf);
      srcs = srcFs.globStatus(srcf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException("addFiles: filesystem error in check phase. " + e.getMessage(), e);
    }
    if (srcs == null) {
      LOG.info("No sources specified to move: " + srcf);
      return;
      // srcs = new FileStatus[0]; Why is this needed?
    }

    // If we're moving files around for an ACID write then the rules and paths are all different.
    // You can blame this on Owen.
    if (isAcidIUD) {
      Hive.moveAcidFiles(srcFs, srcs, destf, newFiles);
    } else {
      // For ACID non-bucketed case, the filenames have to be in the format consistent with INSERT/UPDATE/DELETE Ops,
      // i.e, like 000000_0, 000001_0_copy_1, 000002_0.gz etc.
      // The extension is only maintained for files which are compressed.
      copyFiles(conf, fs, srcs, srcFs, destf, isSrcLocal, isOverwrite,
              newFiles, isFullAcidTable && !isBucketed);
    }
  }

  private void copyFiles(HiveConf conf, FileSystem destFs, FileStatus[] srcs,
                        FileSystem srcFs, Path destf, boolean isSrcLocal,
                        boolean isOverwrite, List<Path> newFiles,
                        boolean acidRename) throws HiveException {
    final HdfsUtils.HadoopFileStatus fullDestStatus;
    try {
      fullDestStatus = new HdfsUtils.HadoopFileStatus(conf, destFs, destf);
    } catch (IOException e1) {
      throw new HiveException(e1);
    }

    if (!fullDestStatus.getFileStatus().isDirectory()) {
      throw new HiveException(destf + " is not a directory.");
    }
    final List<Future<ObjectPair<Path, Path>>> futures = new LinkedList<>();
    final ExecutorService pool = conf.getInt(HiveConf.ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25) > 0 ?
        Executors.newFixedThreadPool(conf.getInt(HiveConf.ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Move-Thread-%d").build()) : null;
    // For ACID non-bucketed case, the filenames have to be in the format consistent with INSERT/UPDATE/DELETE Ops,
    // i.e, like 000000_0, 000001_0_copy_1, 000002_0.gz etc.
    // The extension is only maintained for files which are compressed.
    int taskId = 0;
    // Sort the files
    Arrays.sort(srcs);
    for (FileStatus src : srcs) {
      FileStatus[] files;
      if (src.isDirectory()) {
        try {
          files = srcFs.listStatus(src.getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        } catch (IOException e) {
          if (null != pool) {
            pool.shutdownNow();
          }
          throw new HiveException(e);
        }
      } else {
        files = new FileStatus[] {src};
      }

      final SessionState parentSession = SessionState.get();
      // Sort the files
      Arrays.sort(files);
      for (final FileStatus srcFile : files) {
        final Path srcP = srcFile.getPath();
        final boolean needToCopy = needToCopy(srcP, destf, srcFs, destFs);

        final boolean isRenameAllowed = !needToCopy && !isSrcLocal;

        final String msg = "Unable to move source " + srcP + " to destination " + destf;

        // If we do a rename for a non-local file, we will be transfering the original
        // file permissions from source to the destination. Else, in case of mvFile() where we
        // copy from source to destination, we will inherit the destination's parent group ownership.
        if (null == pool) {
          try {
            Path destPath = mvFile(conf, srcFs, srcP, destFs, destf, isSrcLocal, isOverwrite,
                    isRenameAllowed,
                    acidRename ? taskId++ : -1);

            if (null != newFiles) {
              newFiles.add(destPath);
            }
          } catch (Exception e) {
            throw Hive.getHiveException(e, msg, "Failed to move: {}");
          }
        } else {
          // future only takes final or seemingly final values. Make a final copy of taskId
          final int finalTaskId = acidRename ? taskId++ : -1;
          futures.add(pool.submit(new Callable<ObjectPair<Path, Path>>() {
            @Override
            public ObjectPair<Path, Path> call() throws HiveException {
              SessionState.setCurrentSessionState(parentSession);

              try {
                Path destPath = mvFile(conf, srcFs, srcP, destFs, destf, isSrcLocal, isOverwrite,
                            isRenameAllowed, finalTaskId);

                if (null != newFiles) {
                  newFiles.add(destPath);
                }
                return ObjectPair.create(srcP, destPath);
              } catch (Exception e) {
                throw Hive.getHiveException(e, msg);
              }
            }
          }));
        }
      }
    }
    if (null != pool) {
      pool.shutdown();
      for (Future<ObjectPair<Path, Path>> future : futures) {
        try {
          ObjectPair<Path, Path> pair = future.get();
          LOG.debug("Moved src: {}, to dest: {}", pair.getFirst().toString(), pair.getSecond().toString());
        } catch (Exception e) {
          throw handlePoolException(pool, e);
        }
      }
    }
  }

  /**
   * Replaces files in the partition with new data set specified by srcf. Works
   * by renaming directory of srcf to the destination file.
   * srcf, destf, and tmppath should resident in the same DFS, but the oldPath can be in a
   * different DFS.
   *
   * @param tablePath path of the table.  Used to identify permission inheritance.
   * @param srcf
   *          Source directory to be renamed to tmppath. It should be a
   *          leaf directory where the final data files reside. However it
   *          could potentially contain subdirectories as well.
   * @param destf
   *          The directory where the final data needs to go
   * @param oldPath
   *          The directory where the old data location, need to be cleaned up.  Most of time, will be the same
   *          as destf, unless its across FileSystem boundaries.
   * @param purge
   *          When set to true files which needs to be deleted are not moved to Trash
   * @param isSrcLocal
   *          If the source directory is LOCAL
   * @param newFiles
   *          Output the list of new files replaced in the destination path
   */
  @Override
  public void replaceFiles(Path tablePath, Path srcf, Path destf, Path oldPath, HiveConf conf,
                           boolean isSrcLocal, boolean purge, List<Path> newFiles,
                           PathFilter deletePathFilter,
                           boolean isNeedRecycle, Hive hive) throws HiveException {
    try {

      FileSystem destFs = destf.getFileSystem(conf);
      // check if srcf contains nested sub-directories
      FileStatus[] srcs;
      FileSystem srcFs;
      try {
        srcFs = srcf.getFileSystem(conf);
        srcs = srcFs.globStatus(srcf);
      } catch (IOException e) {
        throw new HiveException("Getting globStatus " + srcf.toString(), e);
      }
      if (srcs == null) {
        LOG.info("No sources specified to move: " + srcf);
        return;
      }

      if (oldPath != null) {
        hive.deleteOldPathForReplace(destf, oldPath, conf, purge, deletePathFilter,
                isNeedRecycle);
      }

      // first call FileUtils.mkdir to make sure that destf directory exists, if not, it creates
      // destf
      boolean destfExist = FileUtils.mkdir(destFs, destf, conf);
      if(!destfExist) {
        throw new IOException("Directory " + destf.toString()
            + " does not exist and could not be created.");
      }

      // Two cases:
      // 1. srcs has only a src directory, if rename src directory to destf, we also need to
      // Copy/move each file under the source directory to avoid to delete the destination
      // directory if it is the root of an HDFS encryption zone.
      // 2. srcs must be a list of files -- ensured by LoadSemanticAnalyzer
      // in both cases, we move the file under destf
      if (srcs.length == 1 && srcs[0].isDirectory()) {
        if (!moveFile(conf, srcs[0].getPath(), destf, true, isSrcLocal)) {
          throw new IOException("Error moving: " + srcf + " into: " + destf);
        }

        // Add file paths of the files that will be moved to the destination if the caller needs it
        if (null != newFiles) {
          listNewFilesRecursively(destFs, destf, newFiles);
        }
      } else {
        // its either a file or glob
        for (FileStatus src : srcs) {
          Path destFile = new Path(destf, src.getPath().getName());
          if (!moveFile(conf, src.getPath(), destFile, true, isSrcLocal)) {
            throw new IOException("Error moving: " + srcf + " into: " + destf);
          }

          // Add file paths of the files that will be moved to the destination if the caller needs it
          if (null != newFiles) {
            newFiles.add(destFile);
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

    /**
   * <p>
   *   Moves a file from one {@link Path} to another. If {@code isRenameAllowed} is true then the
   *   {@link FileSystem#rename(Path, Path)} method is used to move the file. If its false then the data is copied, if
   *   {@code isSrcLocal} is true then the {@link FileSystem#copyFromLocalFile(Path, Path)} method is used, else
   *   {@link FileUtils#copy(FileSystem, Path, FileSystem, Path, boolean, boolean, HiveConf)} is used.
   * </p>
   *
   * <p>
   *   If the destination file already exists, then {@code _copy_[counter]} is appended to the file name, where counter
   *   is an integer starting from 1.
   * </p>
   *
   * @param conf the {@link HiveConf} to use if copying data
   * @param sourceFs the {@link FileSystem} where the source file exists
   * @param sourcePath the {@link Path} to move
   * @param destFs the {@link FileSystem} to move the file to
   * @param destDirPath the {@link Path} to move the file to
   * @param isSrcLocal if the source file is on the local filesystem
   * @param isOverwrite if true, then overwrite destination file if exist else make a duplicate copy
   * @param isRenameAllowed true if the data should be renamed and not copied, false otherwise
   *
   * @return the {@link Path} the source file was moved to
   *
   * @throws IOException if there was an issue moving the file
   */
  private static Path mvFile(HiveConf conf, FileSystem sourceFs, Path sourcePath, FileSystem destFs,
                            Path destDirPath, boolean isSrcLocal, boolean isOverwrite,
                            boolean isRenameAllowed, int taskId) throws IOException {

    // Strip off the file type, if any so we don't make:
    // 000000_0.gz -> 000000_0.gz_copy_1
    final String fullname = sourcePath.getName();
    final String name;
    if (taskId == -1) { // non-acid
      name = FilenameUtils.getBaseName(sourcePath.getName());
    } else { // acid
      name = getPathName(taskId);
    }
    final String type = FilenameUtils.getExtension(sourcePath.getName());

    // Incase of ACID, the file is ORC so the extension is not relevant and should not be inherited.
    Path destFilePath = new Path(destDirPath, taskId == -1 ? fullname : name);

    /*
    * The below loop may perform bad when the destination file already exists and it has too many _copy_
    * files as well. A desired approach was to call listFiles() and get a complete list of files from
    * the destination, and check whether the file exists or not on that list. However, millions of files
    * could live on the destination directory, and on concurrent situations, this can cause OOM problems.
    *
    * I'll leave the below loop for now until a better approach is found.
    */
    for (int counter = 1; destFs.exists(destFilePath); counter++) {
      if (isOverwrite) {
        destFs.delete(destFilePath, false);
        break;
      }
      destFilePath =  new Path(destDirPath, name + (Utilities.COPY_KEYWORD + counter) +
              ((taskId == -1 && !type.isEmpty()) ? "." + type : ""));
    }

    if (isRenameAllowed) {
      destFs.rename(sourcePath, destFilePath);
    } else if (isSrcLocal) {
      destFs.copyFromLocalFile(sourcePath, destFilePath);
    } else {
      FileUtils.copy(sourceFs, sourcePath, destFs, destFilePath,
          true,   // delete source
          false,  // overwrite destination
          conf);
    }
    return destFilePath;
  }

   /**
   * If moving across different FileSystems or differnent encryption zone, need to do a File copy instead of rename.
   * TODO- consider if need to do this for different file authority.
   * @throws HiveException
   */
  static private boolean needToCopy(Path srcf, Path destf, FileSystem srcFs, FileSystem destFs)
          throws HiveException {
    //Check if different FileSystems
    if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
      return true;
    }

    //Check if different encryption zones
    HadoopShims.HdfsEncryptionShim srcHdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim(srcFs);
    HadoopShims.HdfsEncryptionShim destHdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim(destFs);
    try {
      return srcHdfsEncryptionShim != null
          && destHdfsEncryptionShim != null
          && (srcHdfsEncryptionShim.isPathEncrypted(srcf) || destHdfsEncryptionShim.isPathEncrypted(destf))
          && !srcHdfsEncryptionShim.arePathsOnSameEncryptionZone(srcf, destf, destHdfsEncryptionShim);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  static private HiveException handlePoolException(ExecutorService pool, Exception e) {
    HiveException he = null;

    if (e instanceof HiveException) {
      he = (HiveException) e;
      if (he.getCanonicalErrorMsg() != ErrorMsg.GENERIC_ERROR) {
        if (he.getCanonicalErrorMsg() == ErrorMsg.UNRESOLVED_RT_EXCEPTION) {
          LOG.error("Failed to move: {}", he.getMessage());
        } else {
          LOG.error("Failed to move: {}", he.getRemoteErrorMsg());
        }
      }
    } else {
      LOG.error("Failed to move: {}", e.getMessage());
      he = new HiveException(e.getCause());
    }
    pool.shutdownNow();
    return he;
  }

  // List the new files in destination path which gets copied from source.
  private static void listNewFilesRecursively(final FileSystem destFs, Path dest,
                                             List<Path> newFiles) throws HiveException {
    try {
      for (FileStatus fileStatus : destFs.listStatus(dest, FileUtils.HIDDEN_FILES_PATH_FILTER)) {
        if (fileStatus.isDirectory()) {
          // If it is a sub-directory, then recursively list the files.
          listNewFilesRecursively(destFs, fileStatus.getPath(), newFiles);
        } else {
          newFiles.add(fileStatus.getPath());
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get source file statuses", e);
      throw new HiveException(e.getMessage(), e);
    }
  }

  private static String getPathName(int taskId) {
    return Utilities.replaceTaskId("000000", taskId) + "_0";
  }
}
