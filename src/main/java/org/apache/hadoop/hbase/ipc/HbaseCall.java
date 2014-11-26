package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;

/**
 * Handles HBase responses
 */
public class HbaseCall {
  public static final Log LOG = LogFactory.getLog(HbaseCall.class.getName());

  private static int idCounter = 0;

  final int id;

  final Descriptors.MethodDescriptor method;
  final Message param;
  final AsyncPayloadCarryingRpcController controller;
  final Message responseDefaultType;
  final long startTime;
  private final RpcCallback<Message> doneHandler;

  /**
   * Constructor
   *
   * @param md                  the method descriptor
   * @param param               parameters to send to Server
   * @param controller          controller for response
   * @param responseDefaultType the default response type
   * @param doneHandler         done handler
   */
  public HbaseCall(Descriptors.MethodDescriptor md, Message param, AsyncPayloadCarryingRpcController controller,
                   Message responseDefaultType, RpcCallback<Message> doneHandler) {

    this.method = md;
    this.param = param;
    this.controller = controller;
    this.responseDefaultType = responseDefaultType;

    this.startTime = System.currentTimeMillis();

    this.id = idCounter++;

    this.doneHandler = doneHandler;
  }

  /**
   * Get the start time
   *
   * @return start time for the call
   */
  public long getStartTime() {
    return this.startTime;
  }

  @Override
  public String toString() {
    return "callId: " + this.id + " methodName: " + this.method.getName() + " param {" +
        (this.param != null ? ProtobufUtil.getShortTextFormat(this.param) : "") + "}";
  }

  /**
   * Set success with a cellBlockScanner
   *
   * @param value            to set
   * @param cellBlockScanner to set
   */
  public void setSuccess(Message value, CellScanner cellBlockScanner) {
    if (cellBlockScanner != null) {
      controller.setCellScanner(cellBlockScanner);
    }

    if (LOG.isTraceEnabled()) {
      long callTime = System.currentTimeMillis() - startTime;
      if (LOG.isTraceEnabled()) {
        LOG.trace("Call: " + method.getName() + ", callTime: " + callTime + "ms");
      }
    }

    doneHandler.run(value);
  }

  /**
   * Set failed
   *
   * @param exception to set
   */
  public void setFailed(IOException exception) {
    if (ExceptionUtil.isInterrupt(exception)) {
      exception = ExceptionUtil.asInterrupt(exception);
    }
    if (exception instanceof RemoteException) {
      exception = ((RemoteException) exception).unwrapRemoteException();
    }

    controller.setFailed(exception);
  }
}