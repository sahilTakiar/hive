// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Coding Conventions for this file:
//
// Structs/Enums/Unions
// * Struct, Enum, and Union names begin with a "T",
//   and use a capital letter for each new word, with no underscores.
// * All fields should be declared as either optional or required.
//
// Functions
// * Function names start with a capital letter and have a capital letter for
//   each new word, with no underscores.
// * Each function should take exactly one parameter, named TFunctionNameReq,
//   and should return either void or TFunctionNameResp. This convention allows
//   incremental updates.
//
// Services
// * Service names begin with the letter "T", use a capital letter for each
//   new word (with no underscores), and end with the word "Service".

include "TCLIService.thrift"

namespace java org.apache.hive.service.container.thrift
namespace cpp apache.hive.service.container.thrift

service TContainerService {

  TCLIService.TGetInfoResp GetInfo(1:TCLIService.TGetInfoReq req);

  TCLIService.TExecuteStatementResp ExecuteStatement(1:TCLIService.TExecuteStatementReq req);

  TCLIService.TGetOperationStatusResp GetOperationStatus(1:TCLIService.TGetOperationStatusReq req);

  TCLIService.TCancelOperationResp CancelOperation(1:TCLIService.TCancelOperationReq req);

  TCLIService.TCloseOperationResp CloseOperation(1:TCLIService.TCloseOperationReq req);

  TCLIService.TGetResultSetMetadataResp GetResultSetMetadata(1:TCLIService.TGetResultSetMetadataReq req);

  TCLIService.TFetchResultsResp FetchResults(1:TCLIService.TFetchResultsReq req);

  TCLIService.TGetQueryIdResp GetQueryId(1:TCLIService.TGetQueryIdReq req);
}
