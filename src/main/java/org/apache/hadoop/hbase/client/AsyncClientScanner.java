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
package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.ipc.AsyncPayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Implements the scanner interface for the HBase client.
 * If there are multiple regions in a table, this scanner will iterate
 * through them all.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class AsyncClientScanner implements AsyncResultScanner {
  private final Log LOG = LogFactory.getLog(this.getClass());

  final AsyncRpcClient client;

  protected Scan scan;
  protected boolean closed = false;
  // Current region scanner is against.  Gets cleared if current region goes
  // wonky: e.g. if it splits on us.
  protected HRegionInfo currentRegion = null;
  protected AsyncScannerCallable callable = null;

  protected final int caching;
  protected long lastNext;
  // Keep lastResult returned successfully in case we have to reset scanner.
  protected Result lastResult = null;
  protected final long maxScannerResultSize;
  private final TableName tableName;
  protected final int scannerTimeout;
  protected boolean scanMetricsPublished = false;
  protected AsyncRpcRetryingCaller<Result[]> caller;

  protected ScanMetrics scanMetrics;
  private boolean scanIsDone;

  /**
   * Check and initialize if application wants to collect scan metrics
   *
   * @param scan to init it for
   */
  protected void initScanMetrics(Scan scan) {
    // check if application wants to collect scan metrics
    byte[] enableMetrics = scan.getAttribute(
        Scan.SCAN_ATTRIBUTES_METRICS_ENABLE);
    if (enableMetrics != null && Bytes.toBoolean(enableMetrics)) {
      scanMetrics = new ScanMetrics();
    }
  }

  /**
   * @return scan metrics
   */
  protected ScanMetrics getScanMetrics() {
    return scanMetrics;
  }

  /**
   * Create a new ClientScanner for the specified table Note that the passed {@link Scan}'s start
   * row maybe changed changed.
   *
   * @param client    The {@link AsyncRpcClient} to use.
   * @param scan      {@link Scan} to use in this scanner
   * @param tableName The table that we wish to scan
   */
  public AsyncClientScanner(final AsyncRpcClient client, final Scan scan, final TableName tableName) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Scan table=" + tableName
          + ", startRow=" + Bytes.toStringBinary(scan.getStartRow()));
    }
    this.scan = scan;
    this.tableName = tableName;
    this.lastNext = System.currentTimeMillis();
    this.client = client;
    if (scan.getMaxResultSize() > 0) {
      this.maxScannerResultSize = scan.getMaxResultSize();
    } else {
      this.maxScannerResultSize = client.getConfiguration().getLong(
          HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
          HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
    }
    this.scannerTimeout = client.getConfiguration().getInt(
        HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

    // check if application wants to collect scan metrics
    initScanMetrics(scan);

    // Use the caching from the Scan.  If not set, use the default cache setting for this table.
    if (this.scan.getCaching() > 0) {
      this.caching = this.scan.getCaching();
    } else {
      this.caching = client.getConfiguration().getInt(
          HConstants.HBASE_CLIENT_SCANNER_CACHING,
          HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
    }

    this.caller = new AsyncRpcRetryingCaller<>(
        client.getConfiguration().getLong(HConstants.HBASE_CLIENT_PAUSE,
            HConstants.DEFAULT_HBASE_CLIENT_PAUSE),
        client.getConfiguration().getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
            HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER)
    );
  }

  /**
   * @return TableName
   */
  protected TableName getTable() {
    return this.tableName;
  }

  /**
   * @return the used Scan
   */
  protected Scan getScan() {
    return scan;
  }

  /**
   * @return last timestamp
   */
  protected long getTimestamp() {
    return lastNext;
  }

  /**
   * @param endKey to check
   * @return returns true if the passed region endKey
   */
  protected boolean checkScanStopRow(final byte[] endKey) {
    if (this.scan.getStopRow().length > 0) {
      // there is a stop row, check to see if we are past it.
      byte[] stopRow = scan.getStopRow();
      int cmp = Bytes.compareTo(stopRow, 0, stopRow.length,
          endKey, 0, endKey.length);
      if (cmp <= 0) {
        // stopRow <= endKey (endKey is equals to or larger than stopRow)
        // This is a stop.
        return true;
      }
    }
    return false; //unlikely.
  }

  /**
   * Gets a scanner for the next region.  If this.currentRegion != null, then
   * we will move to the endrow of this.currentRegion.  Else we will get
   * scanner at the scan.getStartRow().  We will go no further, just tidy
   * up outstanding scanners, if <code>currentRegion != null</code> and
   * <code>done</code> is true.
   *
   * @param nbRows  number of rows to scan
   * @param done    Server-side says we're done scanning.
   * @param handler for the next scanner
   */
  protected void nextScanner(final int nbRows, final boolean done, final ResponseHandler<Boolean> handler) {
    if (checkToCloseScanner(nbRows, done, handler)) {
      return;
    }

    // Where to start the next scanner
    byte[] localStartKey;

    // if we're at end of table, close and return false to stop iterating
    if (this.currentRegion != null) {
      byte[] endKey = this.currentRegion.getEndKey();
      if (endKey == null ||
          Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY) ||
          checkScanStopRow(endKey) ||
          done) {
        close();
        if (LOG.isTraceEnabled()) {
          LOG.trace("Finished " + this.currentRegion);
        }
        setScanDone();
        handler.onSuccess(false);
      }
      localStartKey = endKey;
      if (LOG.isTraceEnabled()) {
        LOG.trace("Finished " + this.currentRegion);
      }
    } else {
      localStartKey = this.scan.getStartRow();
    }

    if (LOG.isDebugEnabled() && this.currentRegion != null) {
      // Only worth logging if NOT first region in scan.
      LOG.debug("Advancing internal scanner to startKey at '" +
          Bytes.toStringBinary(localStartKey) + "'");
    }
    callable = getScannerCallable(localStartKey, nbRows);
    // Open a scanner on the region server starting at the
    // beginning of the region
    this.caller.callWithRetries(callable, new ResponseHandler<Result[]>() {
      @Override public void onSuccess(Result[] response) {
        currentRegion = callable.getHRegionInfo();
        if (scanMetrics != null) {
          scanMetrics.countOfRegions.incrementAndGet();
        }
        handler.onSuccess(true);
      }

      @Override public void onFailure(IOException e) {
        close();
        handler.onFailure(e);
      }
    });
  }

  /**
   * Checks if scanner should be close
   *
   * @param nbRows  number of rows
   * @param done    done handler
   * @param handler for response
   * @return true if scanner will close and will do next scanner creation itself
   */
  boolean checkToCloseScanner(final int nbRows, final boolean done, final ResponseHandler<Boolean> handler) {
    // Close the previous scanner if it's open
    if (this.callable != null) {
      this.callable.setClose();

      this.caller.callWithRetries(callable, new ResponseHandler<Result[]>() {
        @Override public void onSuccess(Result[] response) {
          callable = null;
          nextScanner(nbRows, done, handler);
        }

        @Override public void onFailure(IOException e) {
          handler.onFailure(e);
        }
      });
      return true;
    }
    return false;
  }

  /**
   * Get the callable for the scan
   *
   * @param localStartKey start key for the scan
   * @param nbRows        number of rows
   * @return async scanner
   */
  @InterfaceAudience.Private
  protected AsyncScannerCallable getScannerCallable(byte[] localStartKey,
                                                    int nbRows) {
    scan.setStartRow(localStartKey);
    AsyncScannerCallable s = new AsyncScannerCallable(this.client,
        getTable(), scan, this.scanMetrics, new AsyncPayloadCarryingRpcController());
    s.setMaxResults(nbRows);
    return s;
  }

  /**
   * Publish the scan metrics. For now, we use scan.setAttribute to pass the metrics back to the
   * application or TableInputFormat.Later, we could push it to other systems. We don't use metrics
   * framework because it doesn't support multi-instances of the same metrics on the same machine;
   * for scan/map reduce scenarios, we will have multiple scans running at the same time.
   * <p/>
   * By default, scan metrics are disabled; if the application wants to collect them, this behavior
   * can be turned on by calling calling:
   * <p/>
   * scan.setAttribute(SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE))
   */
  protected void writeScanMetrics() {
    if (this.scanMetrics == null || scanMetricsPublished) {
      return;
    }
    MapReduceProtos.ScanMetrics pScanMetrics = ProtobufUtil.toScanMetrics(scanMetrics);
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA, pScanMetrics.toByteArray());
    scanMetricsPublished = true;
  }

  @Override public <H extends ResponseHandler<Result[]>> H nextBatch(final H handler) {
    if (callable == null) {
      nextScanner(this.caching, false, new ResponseHandler<Boolean>() {
        @Override public void onSuccess(Boolean response) {
          nextBatch(handler);
        }

        @Override public void onFailure(IOException e) {
          handler.onFailure(e);
        }
      });
    } else {
      callable.setMaxResults(this.caching);

      // Server returns a null values if scanning is to stop.  Else,
      // returns an empty array if scanning is to go on and we've just
      // exhausted current region.
      this.caller.callWithRetries(callable, new RetryingResponseHandler(handler, maxScannerResultSize, this.caching));
    }

    return handler;
  }

  @Override public boolean isScanDone() {
    return scanIsDone;
  }

  /**
   * Set the scanner as done
   */
  protected void setScanDone() {
    this.scanIsDone = true;
  }

  @Override
  public void close() {
    if (!scanMetricsPublished)
      writeScanMetrics();
    if (callable != null) {
      callable.setClose();
      this.caller.callWithRetries(callable, new ResponseHandler<Result[]>() {
        @Override public void onSuccess(Result[] response) {
          callable = null;
          closed = true;
        }

        @Override public void onFailure(IOException e) {
          if (e instanceof UnknownScannerException) {
            // We used to catch this error, interpret, and rethrow. However, we
            // have since decided that it's not nice for a scanner's close to
            // throw exceptions. Chances are it was just due to lease time out
            return;
          } else if (e != null) {
           /* An exception other than UnknownScanner is unexpected. */
            LOG.warn("scanner failed to close. Exception follows: " + e);
          }
          callable = null;
          closed = true;
        }
      });
    }
  }

  /**
   * Handles retrying responses
   */
  private class RetryingResponseHandler implements ResponseHandler<Result[]> {
    private final ResponseHandler<Result[]> handler;
    private long remainingResultSize;
    private int countdown;
    boolean retryAfterOutOfOrderException = true;

    protected final LinkedList<Result> cache = new LinkedList<>();

    /**
     * Constructor
     *
     * @param handler              for if final response comes through
     * @param maxScannerResultSize max result size at which to stop
     * @param caching              number of items to cache
     */
    public RetryingResponseHandler(ResponseHandler<Result[]> handler, long maxScannerResultSize, int caching) {
      this.handler = handler;
      this.remainingResultSize = maxScannerResultSize;
      this.countdown = caching;
    }

    @Override public void onSuccess(Result[] values) {
      retryAfterOutOfOrderException = true;

      long currentTime = System.currentTimeMillis();
      if (scanMetrics != null) {
        scanMetrics.sumOfMillisSecBetweenNexts.addAndGet(currentTime - lastNext);
      }
      lastNext = currentTime;
      if (values != null && values.length > 0) {
        for (Result rs : values) {
          cache.add(rs);
          remainingResultSize--;
          countdown--;
          lastResult = rs;
        }
      }
      tryAgain(values);
    }

    /**
     * Try the call again if more is needed
     *
     * @param values to check for call
     */
    private void tryAgain(Result[] values) {
      // Values == null means server-side filter has determined we must STOP
      if (remainingResultSize > 0 && countdown > 0) {
        nextScanner(countdown, values == null, new ResponseHandler<Boolean>() {
          @Override public void onSuccess(Boolean doContinue) {
            if (doContinue) {
              caller.callWithRetries(callable, RetryingResponseHandler.this);
            } else {
              handler.onSuccess(cache.toArray(new Result[cache.size()]));
              cache.clear();
              // if we exhausted this scanner before calling close, write out the scan metrics
              writeScanMetrics();
            }
          }

          @Override public void onFailure(IOException e) {
            handler.onFailure(e);
          }
        });
      } else {
        if (remainingResultSize == 0) {
          setScanDone();
        }
        handler.onSuccess(cache.toArray(new Result[cache.size()]));
        cache.clear();
        // if we exhausted this scanner before calling close, write out the scan metrics
        writeScanMetrics();
      }
    }

    @Override public void onFailure(IOException e) {
      if (e instanceof DoNotRetryIOException) {
        // DNRIOEs are thrown to make us break out of retries.  Some types of DNRIOEs want us
        // to reset the scanner and come back in again.
        if (e instanceof UnknownScannerException) {
          long timeout = lastNext + scannerTimeout;
          // If we are over the timeout, throw this exception to the client wrapped in
          // a ScannerTimeoutException. Else, it's because the region moved and we used the old
          // id against the new region server; reset the scanner.
          if (timeout < System.currentTimeMillis()) {
            long elapsed = System.currentTimeMillis() - lastNext;
            ScannerTimeoutException ex = new ScannerTimeoutException(
                elapsed + "ms passed since the last invocation, " +
                    "timeout is currently set to " + scannerTimeout);
            ex.initCause(e);
            handler.onFailure(e);
          }
        } else {
          // If exception is any but the list below throw it back to the client; else setup
          // the scanner and retry.
          Throwable cause = e.getCause();
          if ((cause != null && cause instanceof NotServingRegionException) ||
              (cause != null && cause instanceof RegionServerStoppedException) ||
              e instanceof OutOfOrderScannerNextException) {
            // Pass
            // It is easier writing the if loop test as list of what is allowed rather than
            // as a list of what is not allowed... so if in here, it means we do not throw.
          } else {
            handler.onFailure(e);
          }
        }
        // Else, its signal from depths of ScannerCallable that we need to reset the scanner.
        if (lastResult != null) {
          // The region has moved. We need to open a brand new scanner at
          // the new location.
          // Reset the startRow to the row we've seen last so that the new
          // scanner starts at the correct row. Otherwise we may see previously
          // returned rows again.
          // (ScannerCallable by now has "relocated" the correct region)
          scan.setStartRow(lastResult.getRow());

          byte[] newStart = new byte[lastResult.getRow().length + 1];
          System.arraycopy(lastResult.getRow(), 0, newStart, 0, newStart.length - 1);
          newStart[newStart.length - 1] = 0;
          scan.setStartRow(newStart);
        }
        if (e instanceof OutOfOrderScannerNextException) {
          if (retryAfterOutOfOrderException) {
            retryAfterOutOfOrderException = false;
          } else {
            handler.onFailure(new DoNotRetryIOException("Failed after retry of " +
                "OutOfOrderScannerNextException: was there a rpc timeout?", e));
          }
        }
        // Clear region.
        currentRegion = null;
        // Set this to zero so we don't try and do an rpc and close on remote server when
        // the exception we got was UnknownScanner or the Server is going down.
        callable = null;
        // This continue will take us to while at end of loop where we will set up new scanner.
        tryAgain(null);
      }
    }

  }
}