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
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ExceptionUtil;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;

/**
 * A reversed client scanner which support backward scanning
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class AsyncReversedClientScanner extends AsyncClientScanner {
  private static final Log LOG = LogFactory.getLog(ReversedClientScanner.class);
  // A byte array in which all elements are the max byte, and it is used to
  // construct closest front row
  static byte[] MAX_BYTE_ARRAY = Bytes.createMaxByteArray(9);

  /**
   * Create a new ReversibleClientScanner for the specified table Note that the
   * passed {@link Scan}'s start row maybe changed.
   *
   * @param client    The {@link AsyncRpcClient} to use.
   * @param scan      {@link Scan} to use in this scanner
   * @param tableName The table that we wish to scan
   */
  public AsyncReversedClientScanner(final HBaseClient client, final Scan scan, final TableName tableName) {
    super(client, scan, tableName);
  }

  @Override
  protected void nextScanner(final int nbRows, final boolean done, final ResponseHandler<Boolean> handler) {
    if (checkToCloseScanner(nbRows, done, handler)) {
      return;
    }

    // Where to start the next scanner
    byte[] localStartKey;
    boolean locateTheClosestFrontRow = true;
    // if we're at start of table, close and return false to stop iterating
    if (this.currentRegion != null) {
      byte[] startKey = this.currentRegion.getStartKey();
      if (startKey == null
          || Bytes.equals(startKey, HConstants.EMPTY_BYTE_ARRAY)
          || checkScanStopRow(startKey) || done) {
        close();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Finished " + this.currentRegion);
        }
        setScanDone();
        handler.onSuccess(false);
        return;
      }
      localStartKey = startKey;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Finished " + this.currentRegion);
      }
    } else {
      localStartKey = this.scan.getStartRow();
      if (!Bytes.equals(localStartKey, HConstants.EMPTY_BYTE_ARRAY)) {
        locateTheClosestFrontRow = false;
      }
    }

    if (LOG.isDebugEnabled() && this.currentRegion != null) {
      // Only worth logging if NOT first region in scan.
      LOG.debug("Advancing internal scanner to startKey at '"
          + Bytes.toStringBinary(localStartKey) + "'");
    }
    // In reversed scan, we want to locate the previous region through current
    // region's start key. In order to get that previous region, first we
    // create a closest row before the start key of current region, then
    // locate all the regions from the created closest row to start key of
    // current region, thus the last one of located regions should be the
    // previous region of current region. The related logic of locating
    // regions is implemented in ReversedScannerCallable
    byte[] locateStartRow = locateTheClosestFrontRow ? createClosestRowBefore(localStartKey)
        : null;
    callable = getScannerCallable(localStartKey, nbRows, locateStartRow);
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
        try {
          ExceptionUtil.rethrowIfInterrupt(e);
        } catch (InterruptedIOException e1) {
          handler.onFailure(e1);
          return;
        }
        close();
        handler.onFailure(e);
      }
    });
  }

  /**
   * Get scanner callable
   *
   * @param localStartKey  start key to start
   * @param nbRows         rows to scan
   * @param locateStartRow start row
   * @return AsyncScannerCallable
   */
  protected AsyncScannerCallable getScannerCallable(byte[] localStartKey, int nbRows, byte[] locateStartRow) {
    scan.setStartRow(localStartKey);
    AsyncScannerCallable s =
        new AsyncReversedScannerCallable(client, getTable(), scan, this.scanMetrics,
            locateStartRow, new PayloadCarryingRpcController());
    s.setMaxResults(nbRows);
    return s;
  }

  @Override
  // returns true if stopRow >= passed region startKey
  protected boolean checkScanStopRow(final byte[] startKey) {
    if (this.scan.getStopRow().length > 0) {
      // there is a stop row, check to see if we are past it.
      byte[] stopRow = scan.getStopRow();
      int cmp = Bytes.compareTo(stopRow, 0, stopRow.length, startKey, 0,
          startKey.length);
      if (cmp >= 0) {
        // stopRow >= startKey (stopRow is equals to or larger than endKey)
        // This is a stop.
        return true;
      }
    }
    return false; // unlikely.
  }

  /**
   * Create the closest row before the specified row
   *
   * @param row to create row before
   * @return a new byte array which is the closest front row of the specified one
   */
  protected byte[] createClosestRowBefore(byte[] row) {
    if (row == null) {
      throw new IllegalArgumentException("The passed row is empty");
    }
    if (Bytes.equals(row, HConstants.EMPTY_BYTE_ARRAY)) {
      return MAX_BYTE_ARRAY;
    }
    if (row[row.length - 1] == 0) {
      return Arrays.copyOf(row, row.length - 1);
    } else {
      byte[] closestFrontRow = Arrays.copyOf(row, row.length);
      closestFrontRow[row.length - 1] = (byte) ((closestFrontRow[row.length - 1] & 0xff) - 1);
      closestFrontRow = Bytes.add(closestFrontRow, MAX_BYTE_ARRAY);
      return closestFrontRow;
    }
  }
}