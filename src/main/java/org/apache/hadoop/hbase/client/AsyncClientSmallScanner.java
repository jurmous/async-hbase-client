/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.client;

import shaded.hbase.com.google.protobuf.RpcCallback;
import mousio.hbase.async.HBaseClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Client scanner for small scan. Generally, only one RPC is called to fetch the
 * scan results, unless the results cross multiple regions or the row count of
 * results excess the caching.
 * <p/>
 * For small scan, it will get better performance than {@link ClientScanner}
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AsyncClientSmallScanner extends AsyncClientScanner {
  private final Log LOG = LogFactory.getLog(this.getClass());
  private AsyncRegionServerCallable<Result[]> smallScanCallable = null;
  // When fetching results from server, skip the first result if it has the same
  // row with this one
  private byte[] skipRowOfFirstResult = null;

  /**
   * Create a new ShortClientScanner for the specified table Note that the
   * passed {@link Scan}'s start row maybe changed changed.
   *
   * @param client    The {@link org.apache.hadoop.hbase.ipc.AsyncRpcClient} to use.
   * @param scan      {@link Scan} to use in this scanner
   * @param tableName The table that we wish to scan
   */
  public AsyncClientSmallScanner(final HBaseClient client, final Scan scan, final TableName tableName) {
    super(client, scan, tableName);
  }

  /**
   * Gets a scanner for following scan. Move to next region or continue from the
   * last result or start from the start row.
   *
   * @param nbRows            number of rows to fetch
   * @param done              true if Server-side says we're done scanning.
   * @param currentRegionDone true if scan is over on current region
   * @return true if has next scanner
   * @throws IOException if scanner could not be created
   */
  private boolean nextScanner(int nbRows, final boolean done,
                              boolean currentRegionDone) throws IOException {
    // Where to start the next getter
    byte[] localStartKey;
    int cacheNum = nbRows;
    skipRowOfFirstResult = null;
    // if we're at end of table, close and return false to stop iterating
    if (this.currentRegion != null && currentRegionDone) {
      byte[] endKey = this.currentRegion.getEndKey();
      if (endKey == null || Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY)
          || checkScanStopRow(endKey) || done) {
        close();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Finished with small scan at " + this.currentRegion);
        }
        setScanDone();
        return false;
      }
      localStartKey = endKey;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished with region " + this.currentRegion);
      }
    } else if (this.lastResult != null) {
      localStartKey = this.lastResult.getRow();
      skipRowOfFirstResult = this.lastResult.getRow();
      cacheNum++;
    } else {
      localStartKey = this.scan.getStartRow();
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace("Advancing internal small scanner to startKey at '"
          + Bytes.toStringBinary(localStartKey) + "'");
    }
    smallScanCallable = getSmallScanCallable(
        scan, client, getTable(), localStartKey, cacheNum);
    if (this.scanMetrics != null && skipRowOfFirstResult == null) {
      this.scanMetrics.countOfRegions.incrementAndGet();
    }
    return true;
  }

  /**
   * Get small scan callable
   *
   * @param scan          scan to run
   * @param client        to run on
   * @param table         to scan
   * @param localStartKey start key
   * @param cacheNum      cache amount
   * @return Callable
   * @throws IOException if callable creation fails
   */
  static AsyncRegionServerCallable<Result[]> getSmallScanCallable(
      final Scan scan, HBaseClient client, TableName table, byte[] localStartKey,
      final int cacheNum) throws IOException {
    scan.setStartRow(localStartKey);
    return new AsyncRegionServerCallable<Result[]>(
        client, table, scan.getStartRow()) {
      @Override public void call(final ResponseHandler<Result[]> handler) {
        ScanRequest request;
        try {
          request = RequestConverter.buildScanRequest(getLocation()
              .getRegionInfo().getRegionName(), scan, cacheNum, true);
        } catch (IOException e) {
          handler.onFailure(e);
          return;
        }

        final PayloadCarryingRpcController controller = new PayloadCarryingRpcController();
        controller.setPriority(getTableName());
        controller.notifyOnFail(new RpcCallback<IOException>() {
          @Override
          public void run(IOException error) {
            handler.onFailure(error);
          }
        });
        getStub().scan(controller, request, new RpcCallback<ScanResponse>() {
          @Override public void run(ScanResponse response) {
            try {
              handler.onSuccess(ResponseConverter.getResults(controller.cellScanner(), response));
            } catch (IOException e) {
              handler.onFailure(e);
            }
          }
        });
      }
    };
  }

  @Override public <H extends ResponseHandler<Result[]>> H nextBatch(final H handler) {
    try {
      nextScanner(this.caching, true, false);
    } catch (IOException e) {
      handler.onFailure(e);
      return handler;
    }

    // Server returns a null values if scanning is to stop.  Else,
    // returns an empty array if scanning is to go on and we've just
    // exhausted current region.
    this.caller.callWithRetries(smallScanCallable, new RetryingResponseHandler(handler, maxScannerResultSize, this.caching));

    return handler;
  }

  @Override
  public void close() {
    if (!scanMetricsPublished)
      writeScanMetrics();
    closed = true;
  }

  /**
   * Handles retrying responses
   */
  private class RetryingResponseHandler implements ResponseHandler<Result[]> {
    private final ResponseHandler<Result[]> handler;
    private long remainingResultSize;
    private int countdown;
    boolean currentRegionDone = false;
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
      currentRegion = smallScanCallable.getHRegionInfo();
      long currentTime = System.currentTimeMillis();
      if (scanMetrics != null) {
        scanMetrics.sumOfMillisSecBetweenNexts.addAndGet(currentTime
            - lastNext);
      }
      lastNext = currentTime;
      if (values != null && values.length > 0) {
        for (int i = 0; i < values.length; i++) {
          Result rs = values[i];
          if (i == 0 && skipRowOfFirstResult != null
              && Bytes.equals(skipRowOfFirstResult, rs.getRow())) {
            // Skip the first result
            continue;
          }
          cache.add(rs);
          remainingResultSize--;
          countdown--;
          lastResult = rs;
        }
      }
      currentRegionDone = countdown > 0;
      tryAgain(values);
    }

    /**
     * Try the call again if more is needed
     *
     * @param values to check for call
     */
    private void tryAgain(Result[] values) {
      // Values == null means server-side filter has determined we must STOP
      try {
        if (remainingResultSize > 0 && countdown > 0 && nextScanner(countdown, values == null, currentRegionDone)) {
          caller.callWithRetries(callable, RetryingResponseHandler.this);
        } else {
          if (remainingResultSize <= 0) {
            setScanDone();
          }
          handler.onSuccess(cache.toArray(new Result[cache.size()]));
          cache.clear();
          // if we exhausted this scanner before calling close, write out the scan metrics
          writeScanMetrics();
        }
      } catch (IOException e) {
        handler.onFailure(e);
      }
    }

    @Override public void onFailure(IOException e) {
      handler.onFailure(e);
    }
  }
}