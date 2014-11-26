package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;

/**
 * Handles Hbase responses
 */
public class HbaseResponseHandler extends ChannelInboundHandlerAdapter {
  public static final Log LOG = LogFactory.getLog(HbaseResponseHandler.class.getName());

  private final AsyncRpcChannel channel;

  /**
   * Constructor
   *
   * @param channel on which this response handler operates
   */
  public HbaseResponseHandler(AsyncRpcChannel channel) {
    this.channel = channel;
  }

  @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf inBuffer = (ByteBuf) msg;
    ByteBufInputStream in = new ByteBufInputStream(inBuffer);

    if (channel.shouldCloseConnection) {
      return;
    }
    int totalSize = -1;
    try {
      // Read the header
      RPCProtos.ResponseHeader responseHeader = RPCProtos.ResponseHeader.parseDelimitedFrom(in);
      int id = responseHeader.getCallId();
      if (LOG.isDebugEnabled()) {
        LOG.debug(channel.name + ": got response header " +
            TextFormat.shortDebugString(responseHeader) + ", totalSize: " + totalSize + " bytes");
      }
      HbaseCall call = channel.calls.get(id);
      if (call == null) {
        // So we got a response for which we have no corresponding 'call' here on the client-side.
        // We probably timed out waiting, cleaned up all references, and now the server decides
        // to return a response.  There is nothing we can do w/ the response at this stage. Clean
        // out the wire of the response so its out of the way and we can get other responses on
        // this connection.
        int readSoFar = IPCUtil.getTotalSizeWhenWrittenDelimited(responseHeader);
        int whatIsLeftToRead = totalSize - readSoFar;
        LOG.debug("Unknown callId: " + id + ", skipping over this response of " +
            whatIsLeftToRead + " bytes");
        in.skipBytes(whatIsLeftToRead);
      }

      if (responseHeader.hasException()) {
        RPCProtos.ExceptionResponse exceptionResponse = responseHeader.getException();
        RemoteException re = createRemoteException(exceptionResponse);
        if (exceptionResponse.getExceptionClassName().
            equals(FatalConnectionException.class.getName())) {
          channel.close(re);
        } else {
          if (call != null) {
            call.setFailed(re);
          }
        }
      } else {
        Message value = null;
        // Call may be null because it may have timedout and been cleaned up on this side already
        if (call != null && call.responseDefaultType != null) {
          Message.Builder builder = call.responseDefaultType.newBuilderForType();
          builder.mergeDelimitedFrom(in);
          value = builder.build();
        }
        CellScanner cellBlockScanner = null;
        if (responseHeader.hasCellBlockMeta()) {
          int size = responseHeader.getCellBlockMeta().getLength();
          byte[] cellBlock = new byte[size];
          inBuffer.readBytes(cellBlock, 0, cellBlock.length);
          cellBlockScanner = channel.client.createCellScanner(cellBlock);
        }
        // it's possible that this call may have been cleaned up due to a RPC
        // timeout, so check if it still exists before setting the value.
        if (call != null) {
          call.setSuccess(value, cellBlockScanner);
        }
      }
      if (call != null) {
        channel.calls.remove(id);
      }
    } catch (IOException e) {
      // Treat this as a fatal condition and close this connection
      channel.close(e);
    } finally {
      if (channel.rpcTimeout > 0) {
        channel.cleanupTimedOutCalls(channel.rpcTimeout);
      }
    }
  }

  /**
   * @param e Proto exception
   * @return RemoteException made from passed <code>e</code>
   */
  private RemoteException createRemoteException(final RPCProtos.ExceptionResponse e) {
    String innerExceptionClassName = e.getExceptionClassName();
    boolean doNotRetry = e.getDoNotRetry();
    return e.hasHostname() ?
        // If a hostname then add it to the RemoteWithExtrasException
        new RemoteWithExtrasException(innerExceptionClassName,
            e.getStackTrace(), e.getHostname(), e.getPort(), doNotRetry) :
        new RemoteWithExtrasException(innerExceptionClassName,
            e.getStackTrace(), doNotRetry);
  }
}