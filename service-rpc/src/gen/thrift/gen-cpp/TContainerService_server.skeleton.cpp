// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "TContainerService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace  ::apache::hive::service::container::thrift;

class TContainerServiceHandler : virtual public TContainerServiceIf {
 public:
  TContainerServiceHandler() {
    // Your initialization goes here
  }

  void GetInfo( ::apache::hive::service::rpc::thrift::TGetInfoResp& _return, const  ::apache::hive::service::rpc::thrift::TGetInfoReq& req) {
    // Your implementation goes here
    printf("GetInfo\n");
  }

  void ExecuteStatement( ::apache::hive::service::rpc::thrift::TExecuteStatementResp& _return, const  ::apache::hive::service::rpc::thrift::TExecuteStatementReq& req) {
    // Your implementation goes here
    printf("ExecuteStatement\n");
  }

  void GetOperationStatus( ::apache::hive::service::rpc::thrift::TGetOperationStatusResp& _return, const  ::apache::hive::service::rpc::thrift::TGetOperationStatusReq& req) {
    // Your implementation goes here
    printf("GetOperationStatus\n");
  }

  void CancelOperation( ::apache::hive::service::rpc::thrift::TCancelOperationResp& _return, const  ::apache::hive::service::rpc::thrift::TCancelOperationReq& req) {
    // Your implementation goes here
    printf("CancelOperation\n");
  }

  void CloseOperation( ::apache::hive::service::rpc::thrift::TCloseOperationResp& _return, const  ::apache::hive::service::rpc::thrift::TCloseOperationReq& req) {
    // Your implementation goes here
    printf("CloseOperation\n");
  }

  void GetResultSetMetadata( ::apache::hive::service::rpc::thrift::TGetResultSetMetadataResp& _return, const  ::apache::hive::service::rpc::thrift::TGetResultSetMetadataReq& req) {
    // Your implementation goes here
    printf("GetResultSetMetadata\n");
  }

  void FetchResults( ::apache::hive::service::rpc::thrift::TFetchResultsResp& _return, const  ::apache::hive::service::rpc::thrift::TFetchResultsReq& req) {
    // Your implementation goes here
    printf("FetchResults\n");
  }

  void GetQueryId( ::apache::hive::service::rpc::thrift::TGetQueryIdResp& _return, const  ::apache::hive::service::rpc::thrift::TGetQueryIdReq& req) {
    // Your implementation goes here
    printf("GetQueryId\n");
  }

};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<TContainerServiceHandler> handler(new TContainerServiceHandler());
  shared_ptr<TProcessor> processor(new TContainerServiceProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

