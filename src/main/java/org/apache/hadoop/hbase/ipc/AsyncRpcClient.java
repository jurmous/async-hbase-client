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
package org.apache.hadoop.hbase.ipc;

import shaded.hbase.com.google.common.annotations.VisibleForTesting;
import shaded.hbase.com.google.protobuf.Descriptors;
import shaded.hbase.com.google.protobuf.Descriptors.MethodDescriptor;
import shaded.hbase.com.google.protobuf.Message;
import shaded.hbase.com.google.protobuf.RpcCallback;
import shaded.hbase.com.google.protobuf.RpcChannel;
import shaded.hbase.com.google.protobuf.RpcController;

import shaded.hbase.common.io.netty.bootstrap.Bootstrap;
import shaded.hbase.common.io.netty.channel.ChannelInitializer;
import shaded.hbase.common.io.netty.channel.ChannelOption;
import shaded.hbase.common.io.netty.channel.nio.NioEventLoopGroup;
import shaded.hbase.common.io.netty.channel.socket.SocketChannel;
import shaded.hbase.common.io.netty.channel.socket.nio.NioSocketChannel;
import shaded.hbase.common.io.netty.util.HashedWheelTimer;
import shaded.hbase.common.io.netty.util.concurrent.EventExecutor;
import shaded.hbase.common.io.netty.util.concurrent.Future;
import shaded.hbase.common.io.netty.util.concurrent.GenericFutureListener;
import shaded.hbase.common.io.netty.util.concurrent.Promise;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.hbase.util.Threads;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Netty client for the requests and responses
 */
@InterfaceAudience.Private
public class AsyncRpcClient extends AbstractRpcClient {

  public static final HashedWheelTimer WHEEL_TIMER =
      new HashedWheelTimer(100, TimeUnit.MILLISECONDS);

  private static final ChannelInitializer<SocketChannel> DEFAULT_CHANNEL_INITIALIZER =
      new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
          //empty initializer
        }
      };

  protected final AtomicInteger callIdCnt = new AtomicInteger();

  private final NioEventLoopGroup eventLoopGroup;
  private final PoolMap<ConnectionId, AsyncRpcChannel> connections;

  final FailedServers failedServers;

  private final Bootstrap bootstrap;

  /**
   * Constructor for tests
   *
   * @param configuration      to HBase
   * @param clusterId          for the cluster
   * @param localAddress       local address to connect to
   * @param channelInitializer for custom channel handlers
   */
  @VisibleForTesting
  AsyncRpcClient(Configuration configuration, String clusterId, SocketAddress localAddress,
      ChannelInitializer<SocketChannel> channelInitializer) {
    super(configuration, clusterId, localAddress);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting async Hbase RPC client");
    }

    // Max amount of threads to use. 0 lets Netty decide based on amount of cores
    int maxThreads = conf.getInt("hbase.rpc.client.threads.max", 0);

    this.eventLoopGroup = new NioEventLoopGroup(maxThreads,
        Threads.newDaemonThreadFactory("AsyncRpcChannel"));

    this.connections = new PoolMap<>(getPoolType(configuration), getPoolSize(configuration));
    this.failedServers = new FailedServers(configuration);

    int operationTimeout = configuration.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);

    // Configure the default bootstrap.
    this.bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
        .option(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, operationTimeout);
    if (channelInitializer == null) {
      channelInitializer = DEFAULT_CHANNEL_INITIALIZER;
    }
    bootstrap.handler(channelInitializer);
    if (localAddress != null) {
      bootstrap.localAddress(localAddress);
    }
  }

  /**
   * Constructor
   *
   * @param configuration to HBase
   * @param clusterId     for the cluster
   * @param localAddress  local address to connect to
   */
  public AsyncRpcClient(Configuration configuration, String clusterId, SocketAddress localAddress) {
    this(configuration, clusterId, localAddress, null);
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code> which is servicing the <code>protocol</code> protocol,
   * with the <code>ticket</code> credentials, returning the value.
   * Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   *
   * @param ticket Be careful which ticket you pass. A new user will mean a new Connection.
   *               {@link org.apache.hadoop.hbase.security.UserProvider#getCurrent()} makes a new
   *               instance of User each time so will be a new Connection each time.
   * @return A pair with the Message response and the Cell data (if any).
   * @throws InterruptedException if call is interrupted
   * @throws java.io.IOException  if a connection failure is encountered
   */
  @Override
	protected Pair<Message, CellScanner> call(PayloadCarryingRpcController pcrc,
			MethodDescriptor md, Message param, Message returnType, User ticket,
			InetSocketAddress addr) throws IOException, InterruptedException {
	  final AsyncRpcChannel connection = createRpcChannel(md.getService().getName(), addr, ticket);

	    Promise<Message> promise = connection.callMethodWithPromise(md, pcrc, param, returnType);

	    try {
	      Message response = promise.get();
	      return new Pair<>(response, pcrc.cellScanner());
	    } catch (ExecutionException e) {
	      if (e.getCause() instanceof IOException) {
	        throw (IOException) e.getCause();
	      } else {
	        throw new IOException(e.getCause());
	      }
	    }
}
  /**
   * Call method async
   */
  private void callMethod(Descriptors.MethodDescriptor md, final PayloadCarryingRpcController pcrc,
      Message param, Message returnType, User ticket, InetSocketAddress addr,
      final RpcCallback<Message> done) {
    final AsyncRpcChannel connection;
    try {
      connection = createRpcChannel(md.getService().getName(), addr, ticket);

      connection.callMethod(md, pcrc, param, returnType).addListener(
          new GenericFutureListener<Future<Message>>() {
            @Override
            public void operationComplete(Future<Message> future) throws Exception {
              if(!future.isSuccess()){
                Throwable cause = future.cause();
                if (cause instanceof IOException) {
                  pcrc.setFailed((IOException) cause);
                }else{
                  pcrc.setFailed(new IOException(cause));
                }
              }else{
                try {
                  done.run(future.get());
                }catch (ExecutionException e){
                  Throwable cause = e.getCause();
                  if (cause instanceof IOException) {
                    pcrc.setFailed((IOException) cause);
                  }else{
                    pcrc.setFailed(new IOException(cause));
                  }
                }catch (InterruptedException e){
                  pcrc.setFailed(new IOException(e));
                }
              }
            }
          });
    } catch (StoppedRpcClientException|FailedServerException e) {
      pcrc.setFailed(e);
    }
  }

  /**
   * Close netty
   */
  public void close() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping async HBase RPC client");
    }

    synchronized (connections) {
      for (AsyncRpcChannel conn : connections.values()) {
        conn.close(null);
      }
    }

    eventLoopGroup.shutdownGracefully();
  }

  /**
   * Create a cell scanner
   *
   * @param cellBlock to create scanner for
   * @return CellScanner
   * @throws java.io.IOException on error on creation cell scanner
   */
  public CellScanner createCellScanner(byte[] cellBlock) throws IOException {
    return ipcUtil.createCellScanner(this.codec, this.compressor, cellBlock);
  }

  /**
   * Build cell block
   *
   * @param cells to create block with
   * @return ByteBuffer with cells
   * @throws java.io.IOException if block creation fails
   */
  public ByteBuffer buildCellBlock(CellScanner cells) throws IOException {
    return ipcUtil.buildCellBlock(this.codec, this.compressor, cells);
  }

  /**
   * Creates an RPC client
   *
   * @param serviceName    name of servicce
   * @param location       to connect to
   * @param ticket         for current user
   * @return new RpcChannel
   * @throws StoppedRpcClientException when Rpc client is stopped
   * @throws FailedServerException if server failed
   */
  private AsyncRpcChannel createRpcChannel(String serviceName, InetSocketAddress location,
      User ticket) throws StoppedRpcClientException, FailedServerException {
    if (this.eventLoopGroup.isShuttingDown() || this.eventLoopGroup.isShutdown()) {
      throw new StoppedRpcClientException();
    }

    // Check if server is failed
    if (this.failedServers.isFailedServer(location)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not trying to connect to " + location +
            " this server is in the failed servers list");
      }
      throw new FailedServerException(
          "This server is in the failed servers list: " + location);
    }

    ConnectionId id = new ConnectionId(ticket,serviceName,location);

    AsyncRpcChannel rpcChannel;
    synchronized (connections) {
      rpcChannel = connections.get(id);
      if (rpcChannel == null) {
        rpcChannel = new AsyncRpcChannel(this.bootstrap, this, id);
        connections.put(id, rpcChannel);
      }
    }

    return rpcChannel;
  }

  /**
   * Interrupt the connections to the given ip:port server. This should be called if the server
   * is known as actually dead. This will not prevent current operation to be retried, and,
   * depending on their own behavior, they may retry on the same server. This can be a feature,
   * for example at startup. In any case, they're likely to get connection refused (if the
   * process died) or no route to host: i.e. there next retries should be faster and with a
   * safe exception.
   *
   * @param sn server to cancel connections for
   */
  public void cancelConnections(ServerName sn) {
    synchronized (connections) {
      for (AsyncRpcChannel rpcChannel : connections.values()) {
        if (rpcChannel.isAlive() &&
            rpcChannel.address.getPort() == sn.getPort() &&
            rpcChannel.address.getHostName().contentEquals(sn.getHostname())) {
          LOG.info("The server on " + sn.toString() +
              " is dead - stopping the connection " + rpcChannel.toString());
          rpcChannel.close(null);
        }
      }
    }
  }

  /**
   * Remove connection from pool
   *
   * @param connectionId of connection
   */
  public void removeConnection(ConnectionId connectionId) {
    synchronized (connections) {
      this.connections.remove(connectionId);
    }
  }

  /**
   * Creates a "channel" that can be used by a protobuf service.  Useful setting up
   * protobuf stubs.
   *
   * @param sn server name describing location of server
   * @param user which is to use the connection
   * @param rpcTimeout default rpc operation timeout
   *
   * @return A rpc channel that goes via this rpc client instance.
   * @throws IOException when channel could not be created
   */
  public RpcChannel createRpcChannel(final ServerName sn, final User user, int rpcTimeout) {
    return new RpcChannelImplementation(this, sn, user, rpcTimeout);
  }

  /**
   * Get netty event loop
   * @return event loop
   */
  public EventExecutor getEventLoop() {
    return eventLoopGroup.next();
  }

  /**
   * Blocking rpc channel that goes via hbase rpc.
   */
  @VisibleForTesting
  public static class RpcChannelImplementation implements RpcChannel {
    private final InetSocketAddress isa;
    private final AsyncRpcClient rpcClient;
    private final User ticket;
    private final int channelOperationTimeout;

    /**
     * @param channelOperationTimeout - the default timeout when no timeout is given
     */
    protected RpcChannelImplementation(final AsyncRpcClient rpcClient,
        final ServerName sn, final User ticket, int channelOperationTimeout) {
      this.isa = new InetSocketAddress(sn.getHostname(), sn.getPort());
      this.rpcClient = rpcClient;
      this.ticket = ticket;
      this.channelOperationTimeout = channelOperationTimeout;
    }

    @Override
    public void callMethod(Descriptors.MethodDescriptor md, RpcController controller,
        Message param, Message returnType, RpcCallback<Message> done) {
      PayloadCarryingRpcController pcrc;
      if (controller != null) {
        pcrc = (PayloadCarryingRpcController) controller;
        if (!pcrc.hasCallTimeout()) {
          pcrc.setCallTimeout(channelOperationTimeout);
        }
      } else {
        pcrc = new PayloadCarryingRpcController();
        pcrc.setCallTimeout(channelOperationTimeout);
      }

      this.rpcClient.callMethod(md, pcrc, param, returnType, this.ticket, this.isa, done);
    }
  }
}