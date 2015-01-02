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


import mousio.hbase.async.HBaseClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Client scanner for small reversed scan. Generally, only one RPC is called to fetch the
 * scan results, unless the results cross multiple regions or the row count of
 * results exceed the caching.
 * <p/>
 * For small scan, it will get better performance than {@link ReversedClientScanner}
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AsyncClientSmallReversedScanner extends AsyncReversedClientScanner {
  private static final Log LOG = LogFactory.getLog(AsyncClientSmallReversedScanner.class);
  private AsyncRegionServerCallable<Result[]> smallScanCallable = null;
  private byte[] skipRowOfFirstResult = null;

  /**
   * Create a new ReversibleClientScanner for the specified table Note that the
   * passed {@link Scan}'s start row maybe changed.
   *
   * @param client    The {@link org.apache.hadoop.hbase.ipc.AsyncRpcClient} to use.
   * @param scan      {@link Scan} to use in this scanner
   * @param tableName The table that we wish to scan
   */
  public AsyncClientSmallReversedScanner(final HBaseClient client, final Scan scan, final TableName tableName) {
    super(client, scan, tableName);
  }

  /**
   * Gets a scanner for following scan. Move to next region or continue from the
   * last result or start from the start row.
   *
   * @param nbRows            number of rows
   * @param done              true if Server-side says we're done scanning.
   * @param currentRegionDone true if scan is over on current region
   * @return true if has next scanner
   * @throws IOException if scanner creation fails
   */
  private boolean nextScanner(int nbRows, final boolean done,
                              boolean currentRegionDone) throws IOException {
    // Where to start the next getter
    byte[] localStartKey;
    int cacheNum = nbRows;
    skipRowOfFirstResult = null;
    // if we're at end of table, close and return false to stop iterating
    if (this.currentRegion != null && currentRegionDone) {
      byte[] startKey = this.currentRegion.getStartKey();
      if (startKey == null
          || Bytes.equals(startKey, HConstants.EMPTY_BYTE_ARRAY)
          || checkScanStopRow(startKey) || done) {
        close();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Finished with small scan at " + this.currentRegion);
        }
        setScanDone();
        return false;
      }
      // We take the row just under to get to the previous region.
      localStartKey = createClosestRowBefore(startKey);
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

    smallScanCallable = AsyncClientSmallScanner.getSmallScanCallable(scan, client, getTable(),
        localStartKey, cacheNum);

    if (this.scanMetrics != null && skipRowOfFirstResult == null) {
      this.scanMetrics.countOfRegions.incrementAndGet();
    }
    return true;
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
          if (remainingResultSize == 0) {
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