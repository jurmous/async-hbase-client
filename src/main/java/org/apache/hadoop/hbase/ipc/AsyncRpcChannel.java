package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.*;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.protobuf.generated.TracingProtos;
import org.apache.hadoop.hbase.security.AuthMethod;
import org.apache.hadoop.hbase.security.SaslClientHandler;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.cloudera.htrace.Span;
import org.cloudera.htrace.Trace;

import javax.security.sasl.SaslException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

/**
 * Netty RPC channel
 */
public class AsyncRpcChannel implements RpcChannel {
  public static final Log LOG = LogFactory.getLog(AsyncRpcChannel.class.getName());

  private static final byte[] MAGIC = new byte[]{'H', 'B', 'a', 's'};
  private static final int MAX_SASL_RETRIES = 5;

  final AsyncRpcClient client;

  // Contains the channel to work with.
  // Only exists when connected
  private Channel channel;
  // Future connection
  // Exists when in state of connecting
  private ChannelFuture connectFuture;

  String name;
  final RpcClient.ConnectionId remoteId;
  ConcurrentSkipListMap<Integer, HBaseCall> calls = new ConcurrentSkipListMap<>();

  int rpcTimeout;
  private int ioFailureCounter = 0;
  private int connectFailureCounter = 0;

  boolean useSasl;
  AuthMethod authMethod;
  private int reloginMaxBackoff;
  private Token<? extends TokenIdentifier> token;
  private String serverPrincipal;

  protected final static Map<AuthenticationProtos.TokenIdentifier.Kind,
      TokenSelector<? extends TokenIdentifier>> tokenHandlers = new HashMap<>();

  static {
    tokenHandlers.put(AuthenticationProtos.TokenIdentifier.Kind.HBASE_AUTH_TOKEN,
        new AuthenticationTokenSelector());
  }

  public boolean shouldCloseConnection = false;
  private Throwable closeException;

  private Timeout cleanupTimer;

  /**
   * Constructor for netty RPC channel
   *
   * @param bootstrap to construct channel on
   * @param client    to connect with
   * @param remoteId  connection id
   */
  public AsyncRpcChannel(Bootstrap bootstrap, final AsyncRpcClient client, RpcClient.ConnectionId remoteId) {
    this.remoteId = remoteId;
    this.client = client;

    this.name = ("IPC Client connection to " +
        remoteId.getAddress().toString() +
        ((remoteId.getTicket() == null) ? " from an unknown user" : (" from "
            + remoteId.getTicket().getName())));

    connect(bootstrap);
  }

  /**
   * Connect to channel
   *
   * @param bootstrap to connect to
   */
  private void connect(final Bootstrap bootstrap) {
    if (client.failedServers.isFailedServer(remoteId.getAddress())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not trying to connect to " + remoteId.address +
            " this server is in the failed servers list");
      }
      RpcClient.FailedServerException e = new RpcClient.FailedServerException(
          "This server is in the failed servers list: " + remoteId.address);
      close(e);
      return;
    }

    this.connectFuture = bootstrap.remoteAddress(remoteId.address).connect().addListener(new GenericFutureListener<ChannelFuture>() {
      @Override public void operationComplete(final ChannelFuture f) throws Exception {
        if (!f.isSuccess()) {
          if (closeException instanceof SocketException) {
            retryOrClose(bootstrap, connectFailureCounter++, closeException);
          } else {
            retryOrClose(bootstrap, ioFailureCounter++, closeException);
          }
          return;
        }

        setupAuthorization();

        if (useSasl) {
          UserGroupInformation ticket = AsyncRpcChannel.this.remoteId.getTicket().getUGI();
          if (authMethod == AuthMethod.KERBEROS) {
            if (ticket != null && ticket.getRealUser() != null) {
              ticket = ticket.getRealUser();
            }
          }
          SaslClientHandler saslHandler;
          if (ticket == null) {
            throw new FatalConnectionException("ticket/user is null");
          }
          saslHandler = ticket.doAs(new PrivilegedExceptionAction<SaslClientHandler>() {
            @Override
            public SaslClientHandler run() throws IOException {
              return getSaslHandler(bootstrap);
            }
          });
          if (saslHandler != null) {
            // Sasl connect is successful. Let's set up Sasl channel handler
            f.channel().pipeline().addLast(saslHandler);
          } else {
            // fall back to simple auth because server told us so.
            authMethod = AuthMethod.SIMPLE;
            useSasl = false;
          }
        }

        f.channel().pipeline().addLast(new HBaseResponseHandler(AsyncRpcChannel.this));

        writeChannelHeader(f.channel()).addListener(new GenericFutureListener<ChannelFuture>() {
          @Override public void operationComplete(ChannelFuture future) throws Exception {
            connectFuture = null;
            channel = future.channel();
            sendRequestsAfterConnect(channel);
          }
        });

        name = ("IPC Client (" + channel.hashCode() + ") connection to " +
            AsyncRpcChannel.this.remoteId.getAddress().toString() +
            ((AsyncRpcChannel.this.remoteId.ticket == null) ? " from an unknown user" : (" from "
                + AsyncRpcChannel.this.remoteId.ticket.getName())));
      }
    });
  }

  /**
   * Get SASL handler
   *
   * @param bootstrap to reconnect to
   * @return new SASL handler
   * @throws IOException if handler failed to create
   */
  private SaslClientHandler getSaslHandler(final Bootstrap bootstrap) throws IOException {
    return new SaslClientHandler(authMethod, token, serverPrincipal, client.fallbackAllowed,
        client.configuration.get("hbase.rpc.protection", SaslUtil.QualityOfProtection.AUTHENTICATION.name().toLowerCase()),
        new SaslClientHandler.SaslExceptionHandler() {
          @Override public void handle(int retryCount, Random random, Throwable cause) {
            try {
              // Handle Sasl failure. Try to potentially get new credentials
              handleSaslConnectionFailure(retryCount, cause, remoteId.getTicket().getUGI());

              // Try to reconnect
              AsyncRpcClient.WHEEL_TIMER.newTimeout(new TimerTask() {
                @Override public void run(Timeout timeout) throws Exception {
                  connect(bootstrap);
                }
              }, random.nextInt(reloginMaxBackoff) + 1, TimeUnit.MILLISECONDS);
            } catch (IOException | InterruptedException e) {
              close(e);
            }
          }
        });
  }

  /**
   * Retry to connect or close
   *
   * @param bootstrap      to connect with
   * @param connectCounter amount of tries
   * @param e              exception of fail
   */
  private void retryOrClose(final Bootstrap bootstrap, int connectCounter, Throwable e) {
    if (connectCounter < client.maxRetries) {
      AsyncRpcClient.WHEEL_TIMER.newTimeout(new TimerTask() {
        @Override public void run(Timeout timeout) throws Exception {
          connect(bootstrap);
        }
      }, client.failureSleep, TimeUnit.MILLISECONDS);
    } else {
      client.failedServers.addToFailedServers(remoteId.address);
      close(e);
    }
  }

  @Override
  public void callMethod(final Descriptors.MethodDescriptor method, final RpcController controller, final com.google.protobuf.Message request, final Message responsePrototype, final RpcCallback<Message> done) {
    HBaseCall call = new HBaseCall(method, request, (AsyncPayloadCarryingRpcController) controller, responsePrototype, done);
    calls.put(call.id, call);

    if (channel != null) {
      writeRequest(channel, call);
    }
  }

  /**
   * Send all outstanding requests after connecting
   *
   * @param channel to write to
   */
  private void sendRequestsAfterConnect(Channel channel) {
    for (HBaseCall call : calls.values()) {
      writeRequest(channel, call);
    }
  }

  /**
   * Write the channel header
   *
   * @param channel to write to
   * @return future of write
   * @throws IOException on failure to write
   */
  private ChannelFuture writeChannelHeader(Channel channel) throws IOException {
    RPCProtos.ConnectionHeader.Builder headerBuilder = RPCProtos.ConnectionHeader.newBuilder()
        .setServiceName(ClientProtos.ClientService.getDescriptor().getName());

    RPCProtos.ConnectionHeader.Builder builder = RPCProtos.ConnectionHeader.newBuilder();
    builder.setServiceName(remoteId.serviceName);
    RPCProtos.UserInformation userInfoPB;
    if ((userInfoPB = buildUserInfo(remoteId.getTicket().getUGI(), authMethod)) != null) {
      headerBuilder.setUserInfo(userInfoPB);
    }

    if (client.codec != null) {
      headerBuilder.setCellBlockCodecClass(client.codec.getClass().getCanonicalName());
    }
    if (client.compressor != null) {
      headerBuilder.setCellBlockCompressorClass(client.compressor.getClass().getCanonicalName());
    }

    RPCProtos.ConnectionHeader header = headerBuilder.build();

    ByteBuf b = channel.alloc().buffer(6 + IPCUtil.getTotalSizeWhenWrittenDelimited(header));
    createPreamble(b, authMethod);
    b.writeInt(header.getSerializedSize());
    b.writeBytes(header.toByteArray());

    return channel.writeAndFlush(b);
  }

  /**
   * Write request to channel
   *
   * @param channel to write to
   * @param call    to write
   */
  private void writeRequest(Channel channel, HBaseCall call) {
    try {
      if (shouldCloseConnection) {
        return;
      }

      RPCProtos.RequestHeader.Builder builder = RPCProtos.RequestHeader.newBuilder()
          .setCallId(call.id)
          .setMethodName(call.method.getName())
          .setRequestParam(call.param != null);

      if (Trace.isTracing()) {
        Span s = Trace.currentSpan();
        builder.setTraceInfo(TracingProtos.RPCTInfo.newBuilder().
            setParentId(s.getSpanId()).setTraceId(s.getTraceId()));
      }

      ByteBuffer cellBlock = client.buildCellBlock(call.controller.cellScanner());
      if (cellBlock != null) {
        RPCProtos.CellBlockMeta.Builder cellBlockBuilder = RPCProtos.CellBlockMeta.newBuilder();
        cellBlockBuilder.setLength(cellBlock.limit());
        builder.setCellBlockMeta(cellBlockBuilder.build());
      }
      // Only pass priority if there one.  Let zero be same as no priority.
      if (call.controller.getPriority() != 0) {
        builder.setPriority(call.controller.getPriority());
      }

      RPCProtos.RequestHeader rh = builder.build();

      int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(rh, call.param);
      if (cellBlock != null)
        totalSize += cellBlock.remaining();

      ByteBuf b = channel.alloc().buffer(totalSize);

      try (ByteBufOutputStream out = new ByteBufOutputStream(b)) {
        IPCUtil.write(out, rh, call.param, cellBlock);
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug(name + ": wrote request header " + TextFormat.shortDebugString(rh));
      }

      channel.writeAndFlush(b);
    } catch (IOException e) {
      if (!shouldCloseConnection) {
        close(e);
      }
    }
  }

  /**
   * Set up server authorization
   *
   * @throws IOException if auth setup failed
   */
  private void setupAuthorization() throws IOException {
    SecurityInfo securityInfo = SecurityInfo.getInfo(remoteId.serviceName);
    this.useSasl = client.userProvider.isHBaseSecurityEnabled();

    this.token = null;
    if (useSasl && securityInfo != null) {
      AuthenticationProtos.TokenIdentifier.Kind tokenKind = securityInfo.getTokenKind();
      if (tokenKind != null) {
        TokenSelector<? extends TokenIdentifier> tokenSelector =
            tokenHandlers.get(tokenKind);
        if (tokenSelector != null) {
          token = tokenSelector.selectToken(new Text(client.clusterId),
              remoteId.getTicket().getUGI().getTokens());
        } else if (LOG.isDebugEnabled()) {
          LOG.debug("No token selector found for type " + tokenKind);
        }
      }
      String serverKey = securityInfo.getServerPrincipal();
      if (serverKey == null) {
        throw new IOException(
            "Can't obtain server Kerberos config key from SecurityInfo");
      }
      this.serverPrincipal = SecurityUtil.getServerPrincipal(
          client.configuration.get(serverKey), remoteId.address.getAddress().getCanonicalHostName().toLowerCase());
      if (LOG.isDebugEnabled()) {
        LOG.debug("RPC Server Kerberos principal name for service="
            + remoteId.serviceName + " is " + serverPrincipal);
      }
    }

    if (!useSasl) {
      authMethod = AuthMethod.SIMPLE;
    } else if (token != null) {
      authMethod = AuthMethod.DIGEST;
    } else {
      authMethod = AuthMethod.KERBEROS;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Use " + authMethod + " authentication for service " + remoteId.serviceName +
          ", sasl=" + useSasl);
    }
    reloginMaxBackoff = client.configuration.getInt("hbase.security.relogin.maxbackoff", 5000);
  }

  /**
   * Build the user information
   *
   * @param ugi        User Group Information
   * @param authMethod Authorization method
   * @return UserInformation protobuf
   */
  private RPCProtos.UserInformation buildUserInfo(UserGroupInformation ugi, AuthMethod authMethod) {
    if (ugi == null || authMethod == AuthMethod.DIGEST) {
      // Don't send user for token auth
      return null;
    }
    RPCProtos.UserInformation.Builder userInfoPB = RPCProtos.UserInformation.newBuilder();
    if (authMethod == AuthMethod.KERBEROS) {
      // Send effective user for Kerberos auth
      userInfoPB.setEffectiveUser(ugi.getUserName());
    } else if (authMethod == AuthMethod.SIMPLE) {
      //Send both effective user and real user for simple auth
      userInfoPB.setEffectiveUser(ugi.getUserName());
      if (ugi.getRealUser() != null) {
        userInfoPB.setRealUser(ugi.getRealUser().getUserName());
      }
    }
    return userInfoPB.build();
  }

  /**
   * Create connection preamble
   *
   * @param byteBuf    to write to
   * @param authMethod to write
   */
  private void createPreamble(ByteBuf byteBuf, AuthMethod authMethod) {
    byteBuf.writeBytes(MAGIC);
    byteBuf.writeByte(HConstants.RPC_CURRENT_VERSION);
    byteBuf.writeByte(authMethod.code);
  }

  /**
   * Close connection
   *
   * @param e exception on close
   */
  public void close(Throwable e) {
    if (!shouldCloseConnection) {
      shouldCloseConnection = true;
      closeException = e;

      client.removeConnection(remoteId);

      if (channel != null) {
        // log the info
        if (LOG.isDebugEnabled()) {
          LOG.debug(name + ": closing ipc connection to " + channel.remoteAddress() + ": " +
              closeException.getMessage(), closeException);
        }

        if (!channel.isOpen()) {
          channel.close();
        }

      } else if (connectFuture != null) {
        connectFuture.cancel(true);
      }

      cleanupTimedOutCalls(0);

      if (LOG.isDebugEnabled()) {
        LOG.debug(name + ": closed");
      }
    }
  }

  /**
   * Clean up timed out calls
   *
   * @param rpcTimeout for cleanup
   */
  public void cleanupTimedOutCalls(int rpcTimeout) {
    // Cancel outstanding timers
    if (cleanupTimer != null) {
      cleanupTimer.cancel();
      cleanupTimer = null;
    }

    for (HBaseCall call : calls.values()) {
      long waitTime = System.currentTimeMillis() - call.getStartTime();
      if (waitTime >= rpcTimeout) {
        closeException = new RpcClient.CallTimeoutException("Call id=" + call.id +
            ", waitTime=" + waitTime + ", rpcTimeout=" + rpcTimeout);
        call.setFailed((IOException) closeException);
        calls.remove(call.id);
      } else {
        break;
      }
    }
    if (!calls.isEmpty()) {
      HBaseCall firstCall = calls.firstEntry().getValue();
      long maxWaitTime = System.currentTimeMillis() - firstCall.getStartTime();
      if (maxWaitTime < rpcTimeout) {
        rpcTimeout -= maxWaitTime;
      }
    }
    if (!shouldCloseConnection) {
      closeException = null;
      cleanupTimer = AsyncRpcClient.WHEEL_TIMER.newTimeout(new TimerTask() {
        @Override public void run(Timeout timeout) throws Exception {
          cleanupTimer = null;
          cleanupTimedOutCalls(AsyncRpcChannel.this.rpcTimeout);
        }
      }, rpcTimeout, TimeUnit.MILLISECONDS);
    }
  }

  /**
   * Check if the connection is alive
   *
   * @return true if alive
   */
  public boolean isAlive() {
    return channel != null && (channel.isOpen() || channel.isActive());
  }


  /**
   * Check if user should authenticate over Kerberos
   *
   * @return true if should be authenticated over Kerberos
   * @throws IOException on failure of check
   */
  private synchronized boolean shouldAuthenticateOverKrb() throws IOException {
    UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
    UserGroupInformation currentUser =
        UserGroupInformation.getCurrentUser();
    UserGroupInformation realUser = currentUser.getRealUser();
    return authMethod == AuthMethod.KERBEROS &&
        loginUser != null &&
        //Make sure user logged in using Kerberos either keytab or TGT
        loginUser.hasKerberosCredentials() &&
        // relogin only in case it is the login user (e.g. JT)
        // or superuser (like oozie).
        (loginUser.equals(currentUser) || loginUser.equals(realUser));
  }

  /**
   * If multiple clients with the same principal try to connect
   * to the same server at the same time, the server assumes a
   * replay attack is in progress. This is a feature of kerberos.
   * In order to work around this, what is done is that the client
   * backs off randomly and tries to initiate the connection
   * again.
   * The other problem is to do with ticket expiry. To handle that,
   * a relogin is attempted.
   * <p>
   * The retry logic is governed by the {@link #shouldAuthenticateOverKrb}
   * method. In case when the user doesn't have valid credentials, we don't
   * need to retry (from cache or ticket). In such cases, it is prudent to
   * throw a runtime exception when we receive a SaslException from the
   * underlying authentication implementation, so there is no retry from
   * other high level (for eg, HCM or HBaseAdmin).
   * </p>
   *
   * @param currRetries retry count
   * @param ex          exception describing fail
   * @param user        which is trying to connect
   * @throws IOException          if IO fail
   * @throws InterruptedException if thread is interrupted
   */
  private void handleSaslConnectionFailure(
      final int currRetries,
      final Throwable ex,
      final UserGroupInformation user)
      throws IOException, InterruptedException {
    user.doAs(new PrivilegedExceptionAction<Void>() {
      public Void run() throws IOException, InterruptedException {
        if (shouldAuthenticateOverKrb()) {
          if (currRetries < MAX_SASL_RETRIES) {
            LOG.debug("Exception encountered while connecting to the server : " + ex);
            //try re-login
            if (UserGroupInformation.isLoginKeytabBased()) {
              UserGroupInformation.getLoginUser().reloginFromKeytab();
            } else {
              UserGroupInformation.getLoginUser().reloginFromTicketCache();
            }

            // Should reconnect
            return null;
          } else {
            String msg = "Couldn't setup connection for " +
                UserGroupInformation.getLoginUser().getUserName() +
                " to " + serverPrincipal;
            LOG.warn(msg);
            throw (IOException) new IOException(msg).initCause(ex);
          }
        } else {
          LOG.warn("Exception encountered while connecting to " +
              "the server : " + ex);
        }
        if (ex instanceof RemoteException) {
          throw (RemoteException) ex;
        }
        if (ex instanceof SaslException) {
          String msg = "SASL authentication failed." +
              " The most likely cause is missing or invalid credentials." +
              " Consider 'kinit'.";
          LOG.fatal(msg, ex);
          throw new RuntimeException(msg, ex);
        }
        throw new IOException(ex);
      }
    });
  }
}