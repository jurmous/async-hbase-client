package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;

/**
 * Netty Rpc controller
 */
public class AsyncPayloadCarryingRpcController implements RpcController, CellScannable {
  /**
   * Priority to set on this request.  Set it here in controller so available composing the
   * request.  This is the ordained way of setting priorities going forward.  We will be
   * undoing the old annotation-based mechanism.
   */
  // Currently only multi call makes use of this.  Eventually this should be only way to set
  // priority.
  private int priority = 0;

  /**
   * They are optionally set on construction, cleared after we make the call, and then optionally
   * set on response with the result. We use this lowest common denominator access to Cells because
   * sometimes the scanner is backed by a List of Cells and other times, it is backed by an
   * encoded block that implements CellScanner.
   */
  private CellScanner cellScanner;

  private boolean cancelled = false;

  private RpcCallback<IOException> exceptionCallback;
  private IOException exception;

  /**
   * Constructor
   */
  public AsyncPayloadCarryingRpcController() {
    this(null);
  }

  /**
   * Constructor
   *
   * @param cellScanner to init with
   */
  public AsyncPayloadCarryingRpcController(final CellScanner cellScanner) {
    this.cellScanner = cellScanner;
  }

  @Override public void reset() {
    exception = null;
    cancelled = false;
    exceptionCallback = null;
    priority = 0;
    cellScanner = null;
  }

  @Override public boolean failed() {
    return exception != null;
  }

  @Override public String errorText() {
    return exception.getMessage();
  }

  @Override public void startCancel() {
    this.cancelled = true;
  }

  @Override public void setFailed(String reason) {
    this.exception = new IOException(reason);
    if (this.exceptionCallback != null) {
      this.exceptionCallback.run(this.exception);
    }
  }

  @Override public boolean isCanceled() {
    return cancelled;
  }

  @Override public void notifyOnCancel(RpcCallback<Object> callback) {
    // Should only be implemented on the server according to interface
  }

  /**
   * Callback for errors
   *
   * @param callback to run on error
   */
  public void notifyOnError(RpcCallback<IOException> callback) {
    if (this.exception != null) {
      callback.run(this.exception);
    } else {
      this.exceptionCallback = callback;
    }
  }

  @Override public CellScanner cellScanner() {
    return cellScanner;
  }

  /**
   * Set the cell scanner
   *
   * @param cellScanner cell scanner
   */
  public void setCellScanner(CellScanner cellScanner) {
    this.cellScanner = cellScanner;
  }

  /**
   * @param priority Priority for this request; should fall roughly in the range
   *                 {@link org.apache.hadoop.hbase.HConstants#NORMAL_QOS} to {@link org.apache.hadoop.hbase.HConstants#HIGH_QOS}
   */
  public void setPriority(int priority) {
    this.priority = priority;
  }

  /**
   * @param tn Set priority based off the table we are going against.
   */
  public void setPriority(final TableName tn) {
    this.priority = tn != null && tn.isSystemTable() ? HConstants.HIGH_QOS : HConstants.NORMAL_QOS;
  }

  /**
   * @return The priority of this request
   */
  public int getPriority() {
    return priority;
  }

  /**
   * Set failed with an exception
   *
   * @param e exception to set with
   */
  public void setFailed(IOException e) {
    this.exception = e;
    if (this.exceptionCallback != null) {
      this.exceptionCallback.run(this.exception);
    }
  }
}