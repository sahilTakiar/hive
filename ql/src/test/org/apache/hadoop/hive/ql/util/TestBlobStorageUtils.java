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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.ql.util.BlobStorageUtils.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestBlobStorageUtils {
  private static final Configuration conf = new Configuration();

  @Before
  public void setUp() {
    conf.set(HiveConf.ConfVars.HIVE_BLOBSTORE_SUPPORTED_SCHEMES.varname, "s3a,swift");
    conf.setBoolean(HiveConf.ConfVars.HIVE_BLOBSTORE_USE_BLOBSTORE_AS_SCRATCHDIR.varname, false);
  }

  @Test
  public void testValidAndInvalidPaths() throws IOException {
    // Valid paths
    assertTrue(isBlobStoragePath(conf, new Path("s3a://bucket/path")));
    assertTrue(isBlobStoragePath(conf, new Path("swift://bucket/path")));

    // Invalid paths
    assertFalse(isBlobStoragePath(conf, new Path("/tmp/a-path")));
    assertFalse(isBlobStoragePath(conf, new Path("s3fs://tmp/file")));
    assertFalse(isBlobStoragePath(conf, null));
    assertFalse(isBlobStorageFileSystem(conf, null));
    assertFalse(isBlobStoragePath(conf, new Path(URI.create(""))));
  }

  @Test
  public void testValidAndInvalidFileSystems() throws URISyntaxException {
    FileSystem fs = mock(FileSystem.class);

    /* Valid FileSystem schemes */

    doReturn(new URI("s3a:///")).when(fs).getUri();
    assertTrue(isBlobStorageFileSystem(conf, fs));

    doReturn(new URI("swift:///")).when(fs).getUri();
    assertTrue(isBlobStorageFileSystem(conf, fs));

    /* Invalid FileSystem schemes */

    doReturn(new URI("hdfs:///")).when(fs).getUri();
    assertFalse(isBlobStorageFileSystem(conf, fs));

    doReturn(new URI("")).when(fs).getUri();
    assertFalse(isBlobStorageFileSystem(conf, fs));

    assertFalse(isBlobStorageFileSystem(conf, null));
  }

  @Test
  public void testValidAndInvalidSchemes() {
    // Valid schemes
    assertTrue(isBlobStorageScheme(conf, "s3a"));
    assertTrue(isBlobStorageScheme(conf, "swift"));

    // Invalid schemes
    assertFalse(isBlobStorageScheme(conf, "hdfs"));
    assertFalse(isBlobStorageScheme(conf, ""));
    assertFalse(isBlobStorageScheme(conf, null));
  }

  /**
   * Test if {@link BlobStorageUtils#renameDirectoryInParallel(HiveConf, FileSystem, FileSystem, Path, Path, boolean, ExecutorService)}
   * works as specified. The test checks that the directory is successfully renamed.
   */
  @Test
  public void testRenameDirectoryInParallel() throws IOException, HiveException {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Path srcPath = new Path("testRenameDirectoryInParallel-input");
    Path destPath = new Path("testRenameDirectoryInParallel-output");

    String fileName1 = "test-1.txt";
    String fileName2 = "test-2.txt";
    String fileName3 = "test-3.txt";

    HiveConf hiveConf = new HiveConf();
    SessionState.start(hiveConf);

    try {
      localFs.mkdirs(srcPath);
      localFs.mkdirs(destPath);

      localFs.create(new Path(srcPath, fileName1)).close();
      localFs.create(new Path(srcPath, fileName2)).close();
      localFs.create(new Path(srcPath, fileName3)).close();

      BlobStorageUtils.renameDirectoryInParallel(hiveConf, localFs, localFs, srcPath, destPath, true,
              Executors.newFixedThreadPool(1));

      assertTrue(localFs.exists(new Path(destPath, fileName1)));
      assertTrue(localFs.exists(new Path(destPath, fileName2)));
      assertTrue(localFs.exists(new Path(destPath, fileName3)));
    } finally {
      try {
        localFs.delete(srcPath, true);
      } finally {
        localFs.delete(destPath, true);
      }
    }

  }

  /**
   * Test if {@link BlobStorageUtils#renameDirectoryInParallel(HiveConf, FileSystem, FileSystem, Path, Path, boolean, ExecutorService)}
   * works as specified. The test doesn't check the functionality of the method, it only verifies that the method
   * executes the rename requests in parallel.
   */
  @Test
  public void testRenameDirectoryInParallelMockThreadPool() throws IOException, HiveException {
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Path srcPath = new Path("testRenameDirectoryInParallel-input");
    Path destPath = new Path("testRenameDirectoryInParallel-output");

    String fileName1 = "test-1.txt";
    String fileName2 = "test-2.txt";
    String fileName3 = "test-3.txt";

    HiveConf hiveConf = new HiveConf();
    SessionState.start(hiveConf);

    try {
      localFs.mkdirs(srcPath);
      localFs.mkdirs(destPath);

      localFs.create(new Path(srcPath, fileName1)).close();
      localFs.create(new Path(srcPath, fileName2)).close();
      localFs.create(new Path(srcPath, fileName3)).close();

      ExecutorService mockExecutorService = mock(ExecutorService.class);
      when(mockExecutorService.submit(any(Callable.class))).thenAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          Callable callable = (Callable) invocationOnMock.getArguments()[0];
          Future mockFuture = mock(Future.class);
          Object callableResult = callable.call();
          when(mockFuture.get()).thenReturn(callableResult);
          when(mockFuture.get(any(Long.class), any(TimeUnit.class))).thenReturn(callableResult);
          return mockFuture;
        }
      });

      BlobStorageUtils.renameDirectoryInParallel(hiveConf, localFs, localFs, srcPath, destPath, true, mockExecutorService);

      verify(mockExecutorService, times(3)).submit(any(Callable.class));
    } finally {
      try {
        localFs.delete(srcPath, true);
      } finally {
        localFs.delete(destPath, true);
      }
    }
  }
}
