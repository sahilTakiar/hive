/**
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
package org.apache.hadoop.hive.ql.util;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Utilities for different blob (object) storage systems
 */
public class BlobStorageUtils {

    private static final boolean DISABLE_BLOBSTORAGE_AS_SCRATCHDIR = false;
    private static final Logger LOG = LoggerFactory.getLogger(BlobStorageUtils.class);

    public static boolean isBlobStoragePath(final Configuration conf, final Path path) {
        return (path == null) ? false : isBlobStorageScheme(conf, path.toUri().getScheme());
    }

    public static boolean isBlobStorageFileSystem(final Configuration conf, final FileSystem fs) {
        return (fs == null) ? false : isBlobStorageScheme(conf, fs.getUri().getScheme());
    }

    public static boolean isBlobStorageScheme(final Configuration conf, final String scheme) {
        Collection<String> supportedBlobStoreSchemes =
                conf.getStringCollection(HiveConf.ConfVars.HIVE_BLOBSTORE_SUPPORTED_SCHEMES.varname);

        return supportedBlobStoreSchemes.contains(scheme);
    }

    public static boolean isBlobStorageAsScratchDir(final Configuration conf) {
        return conf.getBoolean(
                HiveConf.ConfVars.HIVE_BLOBSTORE_USE_BLOBSTORE_AS_SCRATCHDIR.varname,
                DISABLE_BLOBSTORAGE_AS_SCRATCHDIR
        );
    }

    /**
     * Returns true if a directory should be renamed in parallel, false otherwise.
     */
    public static boolean shouldRenameDirectoryInParallel(final Configuration conf, final FileSystem fs) {
        return HiveConf.getBoolVar(conf,
                HiveConf.ConfVars.HIVE_BLOBSTORE_PARALLEL_DIRECTORY_RENAME) && BlobStorageUtils.isBlobStorageFileSystem(
                fs.getConf(), fs);
    }

    /**
     * Given a source directory and a destination directory, moves all the files under the source to the destination
     * folder. Rename operations are done using the specified {@link ExecutorService}.
     *
     * <p>
     *   This method is useful when running on blob stores where rename operations require copying data from one location
     *   to another. Specifically, this method should be used if the blobstore connector renames files under a directory
     *   sequentially. This method will issue the renames in parallel, which can offer significant performance
     *   improvements.
     * </p>
     *
     * <p>
     *   The source and destination {@link Path}s must be directories.
     * </p>
     *
     * @param hiveConf     the {@link HiveConf} to use when setting permissions
     * @param srcFs        the source {@link FileSystem}
     * @param destFs       the destination {@link FileSystem}
     * @param srcPath      the source {@link Path}
     * @param destPath     the destination {@link Path}
     * @param inheritPerms if true, renamed files with inherit their parent permissions, if false they will preserve
     *                     their original permissions
     * @param pool         the {@link ExecutorService} to use to issue all the {@link FileSystem#rename(Path, Path)}
     *                     requests
     *
     * @throws IOException   if their is an issuing renaming the files
     * @throws HiveException if any other exception occurs while renaming the files
     */
    public static void renameDirectoryInParallel(final HiveConf hiveConf, final FileSystem srcFs,
                                                 final FileSystem destFs, final Path srcPath,
                                                 final Path destPath, final boolean inheritPerms,
                                                 ExecutorService pool) throws IOException, HiveException {

        Preconditions.checkArgument(srcFs.isDirectory(srcPath));
        Preconditions.checkArgument(destFs.isDirectory(destPath));

        final SessionState parentSession = SessionState.get();
        final HdfsUtils.HadoopFileStatus desiredStatus = new HdfsUtils.HadoopFileStatus(destFs.getConf(), destFs,
                destPath);

        List<Future<Void>> futures = new ArrayList<>();

        for (final FileStatus srcStatus : srcFs.listStatus(srcPath)) {
            final Path destFile = new Path(destPath, srcStatus.getPath().getName());

            futures.add(pool.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    SessionState.setCurrentSessionState(parentSession);
                    final String group = srcStatus.getGroup();
                    if (destFs.rename(srcStatus.getPath(), destFile)) {
                        if (inheritPerms) {
                            HdfsUtils.setFullFileStatus(hiveConf, desiredStatus, group, destFs, destFile, false);
                        }
                    } else {
                        throw new IOException("rename for src path: " + srcStatus.getPath() + " to dest path:"
                                + destFile + " returned false");
                    }
                    return null;
                }
            }));
        }

        pool.shutdown();
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                LOG.debug(e.getMessage());
                pool.shutdownNow();
                throw new HiveException(e);
            }
        }
    }
}
