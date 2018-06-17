/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;


/**
 * Similar to {@link HiveSparkClient} except it defines client methods to be used by
 * {@link org.apache.hadoop.hive.ql.SparkRemoteProcessClient}.
 */
public interface RemoteProcessHiveSparkClient extends Serializable {

  CommandProcessorResponse run(String command, byte[] hiveConfBytes);

  boolean getResults(List res) throws IOException;

  CommandProcessorResponse compileAndRespond(String statement, byte[] hiveConfBytes);

  CommandProcessorResponse run();

  boolean hasResultSet();

  Schema getSchema();

  boolean isFetchingTable();

  void close();

  void destroy();

  QueryDisplay getQueryDisplay();
}
