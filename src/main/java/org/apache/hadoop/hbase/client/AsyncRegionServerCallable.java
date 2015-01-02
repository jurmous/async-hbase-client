/**
 *
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

import mousio.hbase.async.HBaseClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;

/**
 * Implementations call a RegionServer and implement {@link #call(ResponseHandler)}.
 * Passed to a {@link RpcRetryingCaller} so we retry on fail.
 *
 * @param <T> the class that the ServerCallable handles
 */
@InterfaceAudience.Private
public abstract class AsyncRegionServerCallable<T> implements AsyncRetryingCallable<T> {
  // Public because used outside of this package over in ipc.
  static final Log LOG = LogFactory.getLog(AsyncRegionServerCallable.class);
  protected final HConnection connection;
  protected final TableName tableName;
  protected final byte[] row;
  protected final HBaseClient client;
  protected HRegionLocation location;
  private ClientService.Interface stub;

  protected final static int MIN_WAIT_DEAD_SERVER = 10000;

  /**
   * @param client    RPC client to use.
   * @param tableName Table name to which <code>row</code> belongs.
   * @param row       The row we want in <code>tableName</code>.
   */
  public AsyncRegionServerCallable(HBaseClient client, TableName tableName, byte[] row) {
    this.connection = client.getConnection();
    this.client = client;
    this.tableName = tableName;
    this.row = row;
  }

  /**
   * Prepare for connection to the server hosting region with row from tablename.  Does lookup
   * to find region location and hosting server.
   *
   * @param reload Set this to true if connection should re-find the region
   * @throws IOException e
   */
  public void prepare(final boolean reload) throws IOException {
    this.location = connection.getRegionLocation(tableName, row, reload);
    if (this.location == null) {
      throw new IOException("Failed to find location, tableName=" + tableName +
          ", row=" + Bytes.toString(row) + ", reload=" + reload);
    }
    setStub(client.getClientService(location));
  }

  /**
   * @return {@link HConnection} instance used by this Callable.
   */
  HConnection getConnection() {
    return this.connection;
  }

  protected ClientService.Interface getStub() {
    return this.stub;
  }

  void setStub(final ClientService.Interface stub) {
    this.stub = stub;
  }

  protected HRegionLocation getLocation() {
    return this.location;
  }

  protected void setLocation(final HRegionLocation location) {
    this.location = location;
  }

  public TableName getTableName() {
    return this.tableName;
  }

  public byte[] getRow() {
    return this.row;
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
    if (t instanceof SocketTimeoutException ||
        t instanceof ConnectException ||
        t instanceof RetriesExhaustedException ||
        (location != null && getConnection().isDeadServer(location.getServerName()))) {
      // if thrown these exceptions, we clear all the cache entries that
      // map to that slow/dead server; otherwise, let cache miss and ask
      // hbase:meta again to find the new location
      if (this.location != null)
        getConnection().clearCaches(location.getServerName());
    } else if (t instanceof RegionMovedException) {
      getConnection().updateCachedLocations(tableName, row, t, location);
    } else if (t instanceof NotServingRegionException && !retrying) {
      // Purge cache entries for this specific region from hbase:meta cache
      // since we don't call connect(true) when number of retries is 1.
      getConnection().deleteCachedRegionLocation(location);
    }
  }

  @Override
  public String getExceptionMessageAdditionalDetail() {
    return "row '" + Bytes.toString(row) + "' on table '" + tableName;
  }

  @Override
  public long sleep(long pause, int tries) {
    // Tries hasn't been bumped up yet so we use "tries + 1" to get right pause time
    long sleep = ConnectionUtils.getPauseTime(pause, tries + 1);
    if (sleep < MIN_WAIT_DEAD_SERVER
        && (location == null || getConnection().isDeadServer(location.getServerName()))) {
      sleep = ConnectionUtils.addJitter(MIN_WAIT_DEAD_SERVER, 0.10f);
    }
    return sleep;
  }

  /**
   * @return the HRegionInfo for the current region
   */
  public HRegionInfo getHRegionInfo() {
    if (this.location == null) {
      return null;
    }
    return this.location.getRegionInfo();
  }
}