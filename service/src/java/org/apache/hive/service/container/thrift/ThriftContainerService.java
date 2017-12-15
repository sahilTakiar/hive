package org.apache.hive.service.container.thrift;

import org.apache.hadoop.hive.common.ServerUtils;
import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.ThreadPoolExecutorWithOomHook;
import org.apache.hive.service.rpc.thrift.TCancelOperationReq;
import org.apache.hive.service.rpc.thrift.TCancelOperationResp;
import org.apache.hive.service.rpc.thrift.TCloseOperationReq;
import org.apache.hive.service.rpc.thrift.TCloseOperationResp;
import org.apache.hive.service.rpc.thrift.TExecuteStatementReq;
import org.apache.hive.service.rpc.thrift.TExecuteStatementResp;
import org.apache.hive.service.rpc.thrift.TFetchResultsReq;
import org.apache.hive.service.rpc.thrift.TFetchResultsResp;
import org.apache.hive.service.rpc.thrift.TGetInfoReq;
import org.apache.hive.service.rpc.thrift.TGetInfoResp;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusReq;
import org.apache.hive.service.rpc.thrift.TGetOperationStatusResp;
import org.apache.hive.service.rpc.thrift.TGetQueryIdReq;
import org.apache.hive.service.rpc.thrift.TGetQueryIdResp;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataReq;
import org.apache.hive.service.rpc.thrift.TGetResultSetMetadataResp;
import org.apache.hive.service.server.HiveServer2;
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

public class ThriftContainerService extends AbstractService implements TContainerService.Iface, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ThriftContainerService.class);

  private static HiveAuthFactory hiveAuthFactory;

  private int portNum;
  private InetAddress serverIPAddress;
  private String hiveHost;
  private TServer server;

  private boolean isStarted = false;
  private boolean isEmbedded = false;

  private HiveConf hiveConf;

  private int minWorkerThreads;
  private int maxWorkerThreads;
  private long workerKeepAliveTime;

  private ThreadLocal<ServerContext> currentServerContext;

  private Runnable oomHook = () -> {};

    static class ThriftContainerServerContext implements ServerContext {
    private SessionHandle sessionHandle = null;

    public void setSessionHandle(SessionHandle sessionHandle) {
      this.sessionHandle = sessionHandle;
    }

    public SessionHandle getSessionHandle() {
      return sessionHandle;
    }
  }

  /**
   * Construct the service.
   *
   * @param name service name
   */
  public ThriftContainerService(String name) {
    super(name);
    currentServerContext = new ThreadLocal<ServerContext>();
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;

    String hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST");
    if (hiveHost == null) {
      hiveHost = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
    }
    try {
      serverIPAddress = ServerUtils.getHostAddress(hiveHost);
    } catch (UnknownHostException e) {
      throw new ServiceException(e);
    }

    // Initialize common server configs needed in both binary & http modes
    String portString;
    // HTTP mode
    if (HiveServer2.isHTTPTransportMode(hiveConf)) {
      workerKeepAliveTime =
          hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_WORKER_KEEPALIVE_TIME,
              TimeUnit.SECONDS);
      portString = System.getenv("HIVE_SERVER2_THRIFT_HTTP_PORT");
      if (portString != null) {
        portNum = Integer.parseInt(portString);
      } else {
        portNum = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT);
      }
    }
    // Binary mode
    else {
      workerKeepAliveTime =
          hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_WORKER_KEEPALIVE_TIME, TimeUnit.SECONDS);
      portString = System.getenv("HIVE_SERVER2_THRIFT_PORT");
      if (portString != null) {
        portNum = Integer.parseInt(portString);
      } else {
        portNum = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT);
      }
    }
    minWorkerThreads = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS);
    maxWorkerThreads = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS);
    super.init(hiveConf);
  }

  @Override
  public void run() {

    // Copied from ThriftBinaryCLIService

    try {
      // Server thread pool
      String threadPoolName = "HiveServer2-Handler-Pool";
      ExecutorService executorService = new ThreadPoolExecutorWithOomHook(minWorkerThreads,
          maxWorkerThreads, workerKeepAliveTime, TimeUnit.SECONDS,
          new SynchronousQueue<Runnable>(), new ThreadFactoryWithGarbageCleanup(threadPoolName),
          oomHook);

      // Thrift configs
      hiveAuthFactory = new HiveAuthFactory(hiveConf);
      TTransportFactory transportFactory = hiveAuthFactory.getAuthTransFactory();
      TProcessorFactory processorFactory = null;//hiveAuthFactory.getAuthProcFactory(this);
      TServerSocket serverSocket = null;
      List<String> sslVersionBlacklist = new ArrayList<String>();
      for (String sslVersion : hiveConf.getVar(HiveConf.ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST).split(",")) {
        sslVersionBlacklist.add(sslVersion);
      }
      if (!hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_USE_SSL)) {
        serverSocket = HiveAuthUtils.getServerSocket(hiveHost, portNum);
      } else {
        String keyStorePath = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim();
        if (keyStorePath.isEmpty()) {
          throw new IllegalArgumentException(HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname
              + " Not configured for SSL connection");
        }
        String keyStorePassword = ShimLoader.getHadoopShims().getPassword(hiveConf,
            HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname);
        serverSocket = HiveAuthUtils.getServerSSLSocket(hiveHost, portNum, keyStorePath,
            keyStorePassword, sslVersionBlacklist);
      }

      // Server args
      int maxMessageSize = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE);
      int requestTimeout = (int) hiveConf.getTimeVar(
          HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT, TimeUnit.SECONDS);
      int beBackoffSlotLength = (int) hiveConf.getTimeVar(
          HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH, TimeUnit.MILLISECONDS);
      TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(serverSocket)
          //.processorFactory(processorFactory)
          .transportFactory(transportFactory)
          .protocolFactory(new TBinaryProtocol.Factory())
          .inputProtocolFactory(new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
          .requestTimeout(requestTimeout).requestTimeoutUnit(TimeUnit.SECONDS)
          .beBackoffSlotLength(beBackoffSlotLength).beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS)
          .executorService(executorService);

      // TCP Server
      server = new TThreadPoolServer(sargs);
      server.setServerEventHandler(new TServerEventHandler() {
        @Override
        public ServerContext createContext(
                TProtocol input, TProtocol output) {
          Metrics metrics = MetricsFactory.getInstance();
          if (metrics != null) {
            try {
              metrics.incrementCounter(MetricsConstant.OPEN_CONNECTIONS);
              metrics.incrementCounter(MetricsConstant.CUMULATIVE_CONNECTION_COUNT);
            } catch (Exception e) {
              LOG.warn("Error Reporting JDO operation to Metrics system", e);
            }
          }
          return new ThriftContainerServerContext();
        }

        @Override
        public void deleteContext(ServerContext serverContext,
          TProtocol input, TProtocol output) {
          Metrics metrics = MetricsFactory.getInstance();
          if (metrics != null) {
            try {
              metrics.decrementCounter(MetricsConstant.OPEN_CONNECTIONS);
            } catch (Exception e) {
              LOG.warn("Error Reporting JDO operation to Metrics system", e);
            }
          }
//          ThriftContainerServerContext context = (ThriftContainerServerContext) serverContext;
//          SessionHandle sessionHandle = context.getSessionHandle();
//          if (sessionHandle != null) {
//            LOG.info("Session disconnected without closing properly. ");
//            try {
//              boolean close = cliService.getSessionManager().getSession(sessionHandle).getHiveConf()
//                .getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_CLOSE_SESSION_ON_DISCONNECT);
//              LOG.info((close ? "" : "Not ") + "Closing the session: " + sessionHandle);
//              if (close) {
//                cliService.closeSession(sessionHandle);
//              }
//            } catch (HiveSQLException e) {
//              LOG.warn("Failed to close session: " + e, e);
//            }
//          }
        }

        @Override
        public void preServe() {
        }

        @Override
        public void processContext(ServerContext serverContext,
                                   TTransport input, TTransport output) {
          currentServerContext.set(serverContext);
        }
      });

      String msg = "Starting " + ThriftContainerService.class.getSimpleName() + " on port "
          + portNum + " with " + minWorkerThreads + "..." + maxWorkerThreads + " worker threads";
      LOG.info(msg);
      server.serve();
    } catch (Throwable t) {
      LOG.error(
          "Error starting HiveServer2: could not start "
              + ThriftContainerService.class.getSimpleName(), t);
      System.exit(-1);
    }
  }

  @Override
  public TGetInfoResp GetInfo(TGetInfoReq req) throws TException {
    return null;
  }

  @Override
  public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq req) throws TException {
    return null;
  }

  @Override
  public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq req) throws TException {
    return null;
  }

  @Override
  public TCancelOperationResp CancelOperation(TCancelOperationReq req) throws TException {
    return null;
  }

  @Override
  public TCloseOperationResp CloseOperation(TCloseOperationReq req) throws TException {
    return null;
  }

  @Override
  public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq req) throws TException {
    return null;
  }

  @Override
  public TFetchResultsResp FetchResults(TFetchResultsReq req) throws TException {
    return null;
  }

  @Override
  public TGetQueryIdResp GetQueryId(TGetQueryIdReq req) throws TException {
    return null;
  }

  public static void main(String args[]) {
    ThriftContainerService containerService = new ThriftContainerService("Container-Service");
    containerService.init(new HiveConf());
    containerService.run();
  }
}
