package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.RpcCallback;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.EventExecutor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.NonceGenerator;
import org.apache.hadoop.hbase.client.ResponseHandler;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hbase.ipc.RpcClient.getPingInterval;

/**
 * Netty client for the requests and responses
 */
public class AsyncRpcClient {
  public static final Log LOG = LogFactory.getLog(AsyncRpcClient.class.getName());

  private final HConnection connection;
  protected final Configuration configuration;

  private final NioEventLoopGroup eventLoopGroup;
  private final PoolMap<RpcClient.ConnectionId, AsyncRpcChannel> connections;

  public final IPCUtil ipcUtil;

  final UserProvider userProvider;
  final String clusterId;

  final RpcClient.FailedServers failedServers;

  private final Bootstrap bootstrap;
  public final Codec codec;
  public final CompressionCodec compressor;
  final boolean fallbackAllowed;

  final int maxRetries;
  final long failureSleep;
  final int rpcTimeout;
  final int maxIdleTime;

  public static final HashedWheelTimer WHEEL_TIMER = new HashedWheelTimer(100, TimeUnit.MILLISECONDS);

  /**
   * Constructor
   *
   * @param connection   to HBase
   * @param clusterId    for the cluster
   * @param localAddress local address to connect to
   */
  public AsyncRpcClient(HConnection connection, String clusterId, SocketAddress localAddress) {
    LOG.info("Setting up Hbase Netty client");

    this.connection = connection;

    this.configuration = connection.getConfiguration();
    this.ipcUtil = new IPCUtil(configuration);

    this.codec = getCodec(configuration);
    this.compressor = getCompressor(configuration);

    this.clusterId = clusterId != null ? clusterId : HConstants.CLUSTER_ID_DEFAULT;

    this.rpcTimeout = configuration.getInt(
        HConstants.HBASE_RPC_TIMEOUT_KEY,
        HConstants.DEFAULT_HBASE_RPC_TIMEOUT);

    this.eventLoopGroup = new NioEventLoopGroup();

    this.maxIdleTime = configuration.getInt("hbase.ipc.client.connection.maxidletime", 10000); //10s
    this.maxRetries = configuration.getInt("hbase.ipc.client.connect.max.retries", 0);
    this.failureSleep = configuration.getLong(HConstants.HBASE_CLIENT_PAUSE,
        HConstants.DEFAULT_HBASE_CLIENT_PAUSE);

    boolean tcpKeepAlive = configuration.getBoolean("hbase.ipc.client.tcpkeepalive", true);
    boolean tcpNoDelay = configuration.getBoolean("hbase.ipc.client.tcpnodelay", true);

    final int pingInterval = getPingInterval(configuration);

    this.connections = new PoolMap<>(RpcClient.getPoolType(configuration), RpcClient.getPoolSize(configuration));
    this.failedServers = new RpcClient.FailedServers(configuration);
    this.fallbackAllowed = configuration.getBoolean(RpcClient.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
        RpcClient.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.userProvider = UserProvider.instantiate(configuration);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Codec=" + this.codec + ", compressor=" + this.compressor +
          ", tcpKeepAlive=" + tcpKeepAlive +
          ", tcpNoDelay=" + tcpNoDelay +
          ", maxIdleTime=" + this.maxIdleTime +
          ", maxRetries=" + this.maxRetries +
          ", fallbackAllowed=" + this.fallbackAllowed +
          ", bind address=" + (localAddress != null ? localAddress : "null"));
    }

    // Configure the default bootstrap.
    this.bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
        .option(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, configuration.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT))
        .handler(new ChannelInitializer<SocketChannel>() {
          @Override
          public void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast("idleStateHandler", new IdleStateHandler(pingInterval, maxIdleTime, 0));
            p.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4));
          }
        });

    if (localAddress != null) {
      bootstrap.localAddress(localAddress);
    }
  }

  /**
   * Encapsulate the ugly casting and RuntimeException conversion in private method.
   *
   * @param configuration to get codec with
   * @return Codec to use on this client.
   */
  Codec getCodec(Configuration configuration) {
    // For NO CODEC, "hbase.client.rpc.codec" must be configured with empty string AND
    // "hbase.client.default.rpc.codec" also -- because default is to do cell block encoding.
    String className = configuration.get(HConstants.RPC_CODEC_CONF_KEY, RpcClient.getDefaultCodec(configuration));
    if (className == null || className.isEmpty())
      return null;
    try {
      return (Codec) Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed getting codec " + className, e);
    }
  }


  /**
   * Encapsulate the ugly casting and RuntimeException conversion in private method.
   *
   * @param conf to use
   * @return The compressor to use on this client.
   */
  private static CompressionCodec getCompressor(final Configuration conf) {
    String className = conf.get("hbase.client.rpc.compressor", null);
    if (className == null || className.isEmpty())
      return null;
    try {
      return (CompressionCodec) Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed getting compressor " + className, e);
    }
  }

  /**
   * Close netty
   */
  public void close() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping async rpc client");
    }

    synchronized (connections) {
      for (AsyncRpcChannel conn : connections.values()) {
        conn.close(new InterruptedException("Closing Async RPC client"));
      }
    }

    eventLoopGroup.shutdownGracefully();

    // wait until all connections are closed
    while (!connections.isEmpty()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignored) {
      }
    }
  }

  /**
   * Get an event loop
   *
   * @return event executor
   */
  public EventExecutor getEventLoop() {
    return eventLoopGroup.next();
  }

  /**
   * Create a cell scanner
   *
   * @param cellBlock to create scanner for
   * @return CellScanner
   * @throws IOException on error on creation cell scanner
   */
  public CellScanner createCellScanner(byte[] cellBlock) throws IOException {
    return ipcUtil.createCellScanner(this.codec, this.compressor, cellBlock);
  }

  /**
   * Build cell block
   *
   * @param cells to create block with
   * @return ByteBuffer with cells
   * @throws IOException if block creation fails
   */
  public ByteBuffer buildCellBlock(CellScanner cells) throws IOException {
    return ipcUtil.buildCellBlock(this.codec, this.compressor, cells);
  }


  /**
   * Interrupt the connections to the given ip:port server. This should be called if the server
   * is known as actually dead. This will not prevent current operation to be retried, and,
   * depending on their own behavior, they may retry on the same server. This can be a feature,
   * for example at startup. In any case, they're likely to get connection refused (if the
   * process died) or no route to host: i.e. there next retries should be faster and with a
   * safe exception.
   *
   * @param hostname to cancel connections for
   * @param port     to cancel connection for
   * @param ioe      exception which canceled connection
   */
  public void cancelConnections(String hostname, int port, IOException ioe) {
    synchronized (connections) {
      for (AsyncRpcChannel rpcChannel : connections.values()) {
        if (rpcChannel.isAlive() &&
            rpcChannel.remoteId.address.getPort() == port &&
            rpcChannel.remoteId.address.getHostName().equals(hostname)) {
          LOG.info("The server on " + hostname + ":" + port +
              " is dead - stopping the connection " + rpcChannel.remoteId);
          rpcChannel.close(ioe);
        }
      }
    }
  }

  /**
   * Get a connection from the pool, or create a new one and add it to the
   * pool.  Connections to a given host/port are reused.
   *
   * @param serviceDescriptor to get connection for
   * @param location          to connect to
   * @return Fitting NettyRpcChannel
   * @throws IOException if connecting fails
   */
  public AsyncRpcChannel getConnection(Descriptors.ServiceDescriptor serviceDescriptor, HRegionLocation location)
      throws IOException {
    if (this.eventLoopGroup.isShuttingDown() || this.eventLoopGroup.isShutdown()) {
      throw new StoppedRpcClientException();
    }
    AsyncRpcChannel rpcChannel;
    RpcClient.ConnectionId remoteId =
        new RpcClient.ConnectionId(
            this.userProvider.getCurrent(),
            serviceDescriptor.getName(),
            new InetSocketAddress(location.getHostname(), location.getPort()),
            this.rpcTimeout);
    synchronized (connections) {
      rpcChannel = connections.get(remoteId);
      if (rpcChannel == null) {
        rpcChannel = new AsyncRpcChannel(
            this.bootstrap,
            this,
            remoteId);
        connections.put(remoteId, rpcChannel);
      }
    }

    return rpcChannel;
  }

  /**
   * Remove connection from pool
   *
   * @param remoteId of connection
   */
  public void removeConnection(RpcClient.ConnectionId remoteId) {
    this.connections.remove(remoteId);
  }

  /**
   * Get hbase configuration
   *
   * @return configuration
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Get region location
   *
   * @param table  to get location of
   * @param row    to get location of
   * @param reload
   * @return Region location
   * @throws java.io.IOException if connection fetching fails
   */
  public HRegionLocation getRegionLocation(TableName table, byte[] row, boolean reload) throws IOException {
    return this.connection.getRegionLocation(table, row, false);
  }

  /**
   * Get nonce generator
   *
   * @return nonce generator
   */
  public NonceGenerator getNonceGenerator() {
    return connection.getNonceGenerator();
  }

  /**
   * Return an new RPC controller
   *
   * @param handler for controller to run on errors
   * @param <T>     Type of Result returned
   * @return new RpcController
   */
  public <T> AsyncPayloadCarryingRpcController newRpcController(final ResponseHandler<T> handler) {
    AsyncPayloadCarryingRpcController controller = new AsyncPayloadCarryingRpcController();
    controller.notifyOnError(new RpcCallback<IOException>() {
      @Override public void run(IOException e) {
        handler.onFailure(e);
      }
    });
    return controller;
  }

  /**
   * Get the client service
   *
   * @param location to get service of
   * @return Service
   * @throws IOException if service creation fails
   */
  public ClientProtos.ClientService.Interface getClientService(HRegionLocation location) throws IOException {
    return ClientProtos.ClientService.newStub(
        getConnection(
            ClientProtos.ClientService.getDescriptor(),
            location
        )
    );
  }

  /**
   * Get the HConnection
   *
   * @return the HConnection
   */
  public HConnection getHConnection() {
    return this.connection;
  }
}