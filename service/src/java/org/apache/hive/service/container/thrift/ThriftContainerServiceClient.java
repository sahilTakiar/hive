package org.apache.hive.service.container.thrift;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ThriftContainerServiceClient {

  private final TContainerService.Iface tContainerService;
  private final SessionHandle sessionHandle;

  public ThriftContainerServiceClient(TContainerService.Iface tContainerService, SessionHandle sessionHandle) {
    this.tContainerService = tContainerService;
    this.sessionHandle = sessionHandle;
  }

  public OperationHandle executeStatement(String statement) throws HiveSQLException {
    try {
      TExecuteStatementReq req =
              new TExecuteStatementReq(this.sessionHandle.toTSessionHandle(), statement);
      TExecuteStatementResp resp = this.tContainerService.ExecuteStatement(req);
      TStatus status = resp.getStatus();
      if (TStatusCode.ERROR_STATUS.equals(status.getStatusCode())) {
        throw new HiveSQLException(status);
      }
      TProtocolVersion protocol = this.sessionHandle.getProtocolVersion();
      return new OperationHandle(resp.getOperationHandle(), protocol);
    } catch (HiveSQLException e) {
      throw e;
    } catch (Exception e) {
      throw new HiveSQLException(e);
    }
  }

  public void open(String host, int port, int timeout) throws TTransportException {
    // Open connection to ContainerService
    // Need some type of protocol to get the host and port number
    // Remote driver needs connect back and deliver the host and port name
    // A better, long-term solution would be for the SparkLauncher to expose it

    TTransport transport = new TSocket(host, port, timeout);
    TProtocol protocol = new TBinaryProtocol(transport);
    TContainerService.Client client = new TContainerService.Client(protocol);
    transport.open();
  }
}
