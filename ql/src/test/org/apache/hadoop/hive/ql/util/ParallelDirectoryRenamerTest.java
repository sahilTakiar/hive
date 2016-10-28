package org.apache.hadoop.hive.ql.util;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.junit.Test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class ParallelDirectoryRenamerTest {

  /**
   * Test if {@link ParallelDirectoryRenamer#renameDirectoryInParallel(HiveConf, FileSystem, FileSystem, Path, Path, boolean, ExecutorService)}
   * works as specified when the destination dir doesn't exist. The test checks that the directory is successfully renamed.
   */
  @Test
  public void testRenameDirectoryInParallelDestNotExists() throws IOException, HiveException {
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

      localFs.create(new Path(srcPath, fileName1)).close();
      localFs.create(new Path(srcPath, fileName2)).close();
      localFs.create(new Path(srcPath, fileName3)).close();

      ParallelDirectoryRenamer.renameDirectoryInParallel(hiveConf, localFs, localFs, srcPath, destPath, true,
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
   * Test if {@link ParallelDirectoryRenamer#renameDirectoryInParallel(HiveConf, FileSystem, FileSystem, Path, Path, boolean, ExecutorService)}
   * works as specified when the destination dir does exist. The test checks that the directory is successfully renamed.
   */
  @Test
  public void testRenameDirectoryInParallelDestExists() throws IOException, HiveException {
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

      ParallelDirectoryRenamer.renameDirectoryInParallel(hiveConf, localFs, localFs, srcPath, destPath, true,
              Executors.newFixedThreadPool(1));

      Path basePath = new Path(destPath, srcPath.getName());
      assertTrue(localFs.exists(new Path(basePath, fileName1)));
      assertTrue(localFs.exists(new Path(basePath, fileName2)));
      assertTrue(localFs.exists(new Path(basePath, fileName3)));
    } finally {
      try {
        localFs.delete(srcPath, true);
      } finally {
        localFs.delete(destPath, true);
      }
    }
  }

  /**
   * Test if {@link ParallelDirectoryRenamer#renameDirectoryInParallel(HiveConf, FileSystem, FileSystem, Path, Path, boolean, ExecutorService)}
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

      ParallelDirectoryRenamer.renameDirectoryInParallel(hiveConf, localFs, localFs, srcPath, destPath, true,
              mockExecutorService);

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
