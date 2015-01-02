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

import com.google.protobuf.RpcCallback;
import com.google.protobuf.TextFormat;
import mousio.hbase.async.HBaseClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.ipc.AsyncPayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.DNS;

import java.io.IOException;
import java.net.UnknownHostException;

/**
 * Scanner operations such as create, next, etc.
 * Used by {@link ResultScanner}s made by {@link HTable}. Passed to a retrying caller such as
 * {@link AsyncRpcRetryingCaller} so fails are retried.
 */
@InterfaceAudience.Private
public class AsyncScannerCallable extends AsyncRegionServerCallable<Result[]> {
  static final Log LOG = LogFactory.getLog(AsyncScannerCallable.class);
  private long scannerId = -1L;
  protected boolean instantiated = false;
  private boolean closed = false;
  private final Scan scan;
  private int maxResults = 1;
  protected ScanMetrics scanMetrics;
  private boolean logScannerActivity = false;
  private int logCutOffLatency = 1000;
  private static String myAddress;

  static {
    try {
      myAddress = DNS.getDefaultHost("default", "default");
    } catch (UnknownHostException uhe) {
      LOG.error("cannot determine my address", uhe);
    }
  }

  // indicate if it is a remote server call
  protected boolean isRegionServerRemote = true;
  private long nextCallSeq = 0;
  protected final AsyncPayloadCarryingRpcController controller;

  /**
   * @param client      to connect with
   * @param tableName   table callable is on
   * @param scan        the scan to execute
   * @param scanMetrics the ScanMetrics to used, if it is null, ScannerCallable won't collect
   *                    metrics
   * @param controller  to use when writing the rpc
   */
  public AsyncScannerCallable(HBaseClient client, TableName tableName, Scan scan,
                              ScanMetrics scanMetrics, AsyncPayloadCarryingRpcController controller) {
    super(client, tableName, scan.getStartRow());
    this.scan = scan;
    this.scanMetrics = scanMetrics;
    Configuration conf = client.getConnection().getConfiguration();
    logScannerActivity = conf.getBoolean(ScannerCallable.LOG_SCANNER_ACTIVITY, false);
    logCutOffLatency = conf.getInt(ScannerCallable.LOG_SCANNER_LATENCY_CUTOFF, 1000);
    this.controller = controller;
  }

  /**
   * @param reload force reload of server location
   * @throws IOException if preparation fails
   */
  public void prepare(boolean reload) throws IOException {
    if (!instantiated || reload) {
      super.prepare(reload);

      checkIfRegionServerIsRemote();
      instantiated = true;
    }

    // check how often we retry.
    // HConnectionManager will call instantiateServer with reload==true
    // if and only if for retries.
    if (reload && this.scanMetrics != null) {
      this.scanMetrics.countOfRPCRetries.incrementAndGet();
      if (isRegionServerRemote) {
        this.scanMetrics.countOfRemoteRPCRetries.incrementAndGet();
      }
    }
  }

  /**
   * compare the local machine hostname with region server's hostname
   * to decide if hbase client connects to a remote region server
   */
  protected void checkIfRegionServerIsRemote() {
    isRegionServerRemote = !location.getHostname().equalsIgnoreCase(myAddress);
  }

  /**
   * Calls the scan request
   *
   * @param handler to handle results
   */
  @SuppressWarnings("deprecation")
  public void call(final ResponseHandler<Result[]> handler) {
    if (closed) {
      if (scannerId != -1) {
        close(handler);
      }
    } else {
      if (scannerId == -1L) {
        openScanner(new ResponseHandler<Long>() {
          @Override public void onSuccess(Long response) {
            scannerId = response;
            handler.onSuccess(null);
          }

          @Override public void onFailure(IOException e) {
            handler.onFailure(e);
          }
        });
      } else {
        incRPCcallsMetrics();
        final ScanRequest request = RequestConverter.buildScanRequest(scannerId, maxResults, false, nextCallSeq);
        controller.setPriority(tableName);
        controller.notifyOnFail(new RpcCallback<IOException>() {
          @Override
          public void run(IOException e) {
            if (logScannerActivity) {
              LOG.info(
                  "Got exception making request " + TextFormat.shortDebugString(request) + " to "
                      + location, e);
            }
            if (e instanceof RemoteException) {
              try {
                e = RemoteExceptionHandler.decodeRemoteException((RemoteException) e);
              } catch (IOException e1) {
                handler.onFailure(e1);
                return;
              }
            }
            if (logScannerActivity && (e instanceof UnknownScannerException)) {
              try {
                HRegionLocation newLocation =
                    client.getConnection().relocateRegion(tableName, row);
                LOG.info(
                    "Scanner=" + scannerId + " expired, current region location is " + newLocation
                        .toString());
              } catch (Throwable t) {
                LOG.info("Failed to relocate region", t);
              }
            }
            // The below convertion of exceptions into DoNotRetryExceptions is a little strange.
            // Why not just have these exceptions implment DNRIOE you ask?  Well, usually we want
            // ServerCallable#withRetries to just retry when it gets these exceptions.  In here in
            // a scan when doing a next in particular, we want to break out and get the scanner to
            // reset itself up again.  Throwing a DNRIOE is how we signal this to happen (its ugly,
            // yeah and hard to follow and in need of a refactor).
            if (e instanceof NotServingRegionException) {
              // Throw a DNRE so that we break out of cycle of calling NSRE
              // when what we need is to open scanner against new location.
              // Attach NSRE to signal client that it needs to re-setup scanner.
              if (scanMetrics != null) {
                scanMetrics.countOfNSRE.incrementAndGet();
              }
              handler.onFailure(
                  new DoNotRetryIOException("Resetting the scanner -- see exception cause", e));
            } else if (e instanceof RegionServerStoppedException) {
              // Throw a DNRE so that we break out of cycle of the retries and instead go and
              // open scanner against new location.
              handler.onFailure(
                  new DoNotRetryIOException("Resetting the scanner -- see exception cause", e));
            } else {
              // The outer layers will retry
              handler.onFailure(e);
            }
          }
        });

        getStub().scan(controller, request, new RpcCallback<ScanResponse>() {
          @Override public void run(ScanResponse response) {
            // Client and RS maintain a nextCallSeq number during the scan. Every next() call
            // from client to server will increment this number in both sides. Client passes this
            // number along with the request and at RS side both the incoming nextCallSeq and its
            // nextCallSeq will be matched. In case of a timeout this increment at the client side
            // should not happen. If at the server side fetching of next batch of data was over,
            // there will be mismatch in the nextCallSeq number. Server will throw
            // OutOfOrderScannerNextException and then client will reopen the scanner with startrow
            // as the last successfully retrieved row.
            // See HBASE-5974
            nextCallSeq++;
            long timestamp = System.currentTimeMillis();
            // Results are returned via controller
            CellScanner cellScanner = controller.cellScanner();
            try {
              Result[] rrs = ResponseConverter.getResults(cellScanner, response);
              if (logScannerActivity) {
                long now = System.currentTimeMillis();
                if (now - timestamp > logCutOffLatency) {
                  int rows = rrs == null ? 0 : rrs.length;
                  LOG.info("Took " + (now - timestamp) + "ms to fetch "
                      + rows + " rows from scanner=" + scannerId);
                }
              }
              if (response.hasMoreResults()
                  && !response.getMoreResults()) {
                scannerId = -1L;
                closed = true;
                rrs = null;
              }

              updateResultsMetrics(rrs);
              handler.onSuccess(rrs);
            } catch (IOException e) {
              handler.onFailure(e);
            }
          }
        });
      }
    }
  }

  /**
   * Increase RPC call metrics
   */

  private void incRPCcallsMetrics() {
    if (this.scanMetrics == null) {
      return;
    }
    this.scanMetrics.countOfRPCcalls.incrementAndGet();
    if (isRegionServerRemote) {
      this.scanMetrics.countOfRemoteRPCcalls.incrementAndGet();
    }
  }

  /**
   * Update results metrics
   *
   * @param rrs results
   */
  private void updateResultsMetrics(Result[] rrs) {
    if (this.scanMetrics == null || rrs == null || rrs.length == 0) {
      return;
    }
    long resultSize = 0;
    for (Result rr : rrs) {
      for (Cell kv : rr.rawCells()) {
        resultSize += CellUtil.estimatedSizeOf(kv);
      }
    }
    this.scanMetrics.countOfBytesInResults.addAndGet(resultSize);
    if (isRegionServerRemote) {
      this.scanMetrics.countOfBytesInRemoteResults.addAndGet(resultSize);
    }
  }

  /**
   * Close the callable
   *
   * @param handler to run on success
   */
  private void close(final ResponseHandler<Result[]> handler) {
    if (this.scannerId == -1L) {
      return;
    }
    incRPCcallsMetrics();
    ScanRequest request =
        RequestConverter.buildScanRequest(this.scannerId, 0, true);
    final ResponseHandler<Result[]> handlerInternal = new ResponseHandler<Result[]>() {
      @Override public void onSuccess(Result[] response) {
        scannerId = -1L;
        closed = true;
        handler.onSuccess(response);
      }

      @Override public void onFailure(IOException e) {
        handler.onFailure(e);
      }
    };

    getStub().scan(client.getNewRpcController(handlerInternal), request,
        new RpcCallback<ScanResponse>() {
          @Override
          public void run(ScanResponse parameter) {
            handlerInternal.onSuccess(null);
          }
        });
  }

  /**
   * Open the scanner
   *
   * @param responseHandler handles response
   */
  protected void openScanner(final ResponseHandler<Long> responseHandler) {
    incRPCcallsMetrics();
    try {
      ScanRequest request = RequestConverter.buildScanRequest(
          location.getRegionInfo().getRegionName(), this.scan, 0, false);
      final ResponseHandler<ScanResponse> handler = new ResponseHandler<ClientProtos.ScanResponse>() {
        @Override public void onSuccess(ClientProtos.ScanResponse response) {
          long id = response.getScannerId();
          if (logScannerActivity) {
            LOG.info("Open scanner=" + id + " for scan=" + scan.toString()
                + " on region " + location.toString());
          }
          responseHandler.onSuccess(id);
        }

        @Override public void onFailure(IOException e) {
          responseHandler.onFailure(e);
        }
      };

      getStub().scan(client.getNewRpcController(handler), request, new RpcCallback<ScanResponse>() {
        @Override public void run(ScanResponse response) {
          handler.onSuccess(response);
        }
      });
    } catch (IOException e) {
      responseHandler.onFailure(e);
    }
  }

  /**
   * Get scan of the callable
   *
   * @return Scan
   */
  protected Scan getScan() {
    return scan;
  }

  /**
   * Call this when the next invocation of call should close the scanner
   */
  public void setClose() {
    this.closed = true;
  }

  /**
   * @return the HRegionInfo for the current region
   */
  public HRegionInfo getHRegionInfo() {
    if (!instantiated) {
      return null;
    }
    return location.getRegionInfo();
  }

  /**
   * Get the number of rows that will be fetched on next
   *
   * @return the number of rows for maxResults
   */
  public int getMaxResults() {
    return maxResults;
  }

  /**
   * Set the number of rows that will be fetched on next
   *
   * @param maxResults the number of rows for maxResults
   */
  public void setMaxResults(int maxResults) {
    this.maxResults = maxResults;
  }
}