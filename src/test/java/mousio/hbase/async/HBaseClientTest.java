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

package mousio.hbase.async;

import com.google.protobuf.RpcCallback;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.apache.hadoop.hbase.protobuf.ProtobufUtil.toResult;
import static org.apache.hadoop.hbase.protobuf.RequestConverter.buildGetRequest;
import static org.junit.Assert.*;

@Category(MediumTests.class)
public class HBaseClientTest {
  public static final TableName TEST_TABLE = TableName.valueOf("TestTable");
  private static final HBaseTestingUtility util = new HBaseTestingUtility();
  private static final byte[] DEFAULT_FAMILY = "f".getBytes();
  private static HBaseClient client;
  private static Connection connection;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util.getConfiguration()
        .set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, AsyncRpcClient.class.getName());
    util.getConfiguration()
        .set(HConnection.HBASE_CLIENT_CONNECTION_IMPL, AsyncConnectionImpl.class.getName());

    util.startMiniCluster();
    util.getMiniHBaseCluster();

    HTableDescriptor desc = new HTableDescriptor(TEST_TABLE);
    util.createTable(desc, new byte[][]{DEFAULT_FAMILY}, util.getConfiguration());

    connection = ConnectionFactory.createConnection(util.getConfiguration());
    client = new HBaseClient(connection);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
	  client.close();
	  util.shutdownMiniCluster();
  }

  @Test
  public void testEmptyGet() throws Exception {
    byte[] row = new byte[]{1};
    Result result = client.get(TEST_TABLE, new Get(row), client.<Result>newPromise()).get();
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testPutAndGet() throws Exception {
    byte[] row = new byte[]{2};
    Put put = new Put(row).add(DEFAULT_FAMILY, new byte[] { 3, 4, 5 }, new byte[] { 6, 7, 8 });
    client.put(TEST_TABLE, put, client.<Void>newPromise()).get();
    Get get = new Get(row);
    Result result = client.get(TEST_TABLE, get, client.<Result>newPromise()).get();
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertArrayEquals(row, result.getRow());
    assertTrue(result.containsColumn(DEFAULT_FAMILY, new byte[] { 3, 4, 5 }));
  }

  @Test
  public void testIncrement() throws Exception {
    byte[] row = new byte[]{3};

    Put put = new Put(row);
    put.add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(2L));
    client.put(TEST_TABLE, put, client.<Void>newPromise()).get();
    Increment increment = new Increment(row).addColumn(DEFAULT_FAMILY, new byte[] { 1 }, 2);
    Result result = client.increment(TEST_TABLE, increment, client.<Result>newPromise()).get();
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertArrayEquals(row, result.getRow());
    assertTrue(result.containsColumn(DEFAULT_FAMILY, new byte[] { 1 }));
    assertEquals(4L, Bytes
        .toLong(CellUtil.cloneValue(result.getColumnLatestCell(DEFAULT_FAMILY, new byte[] { 1 }))));
  }

  @Test
  public void testIncrementValue() throws Exception {
    byte[] row = new byte[]{4};

    Put put = new Put(row);
    put.add(DEFAULT_FAMILY, new byte[]{1}, Bytes.toBytes(5L));
    client.put(TEST_TABLE, put, client.<Void>newPromise()).get();
    Long l = client.incrementColumnValue(TEST_TABLE, row, DEFAULT_FAMILY, new byte[]{1}, 2, client.<Long>newPromise()).get();
    assertEquals(7L, l.longValue());
  }

  @Test
  public void testAppend() throws Exception {
    byte[] row = new byte[]{5};

    Put put = new Put(row);
    put.add(DEFAULT_FAMILY, new byte[]{1}, Bytes.toBytes(2L));
    client.put(TEST_TABLE, put, client.<Void>newPromise()).get();
    Append append = new Append(row);
    append.add(DEFAULT_FAMILY, new byte[]{2}, Bytes.toBytes(5L));
    Result result = client.append(TEST_TABLE, append, client.<Result>newPromise()).get();
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertArrayEquals(row, result.getRow());
    assertTrue(result.containsColumn(DEFAULT_FAMILY, new byte[]{2}));
    assertEquals(5L, Bytes.toLong(
        CellUtil.cloneValue(result.getColumnLatestCell(DEFAULT_FAMILY, new byte[] { 2 }))));
  }

  @Test
  public void testDelete() throws Exception {
    byte[] row = new byte[]{6};

    Put put = new Put(row);
    put.add(DEFAULT_FAMILY, new byte[]{1}, Bytes.toBytes(2L));
    client.put(TEST_TABLE, put, client.<Void>newPromise()).get();
    client.delete(TEST_TABLE, new Delete(row), client.<Void>newPromise()).get();
    Result result = client.get(TEST_TABLE, new Get(row), client.<Result>newPromise()).get();
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testMultiplePutGetDelete() throws Exception {
    byte[] row1 = new byte[]{7, 1};
    byte[] row2 = new byte[]{7, 2};
    byte[] row3 = new byte[]{7, 3};

    client.put(TEST_TABLE, Arrays.asList(
        new Put(row1).add(DEFAULT_FAMILY, new byte[]{1}, Bytes.toBytes(1L)),
        new Put(row2).add(DEFAULT_FAMILY, new byte[]{1}, Bytes.toBytes(2L)),
        new Put(row3).add(DEFAULT_FAMILY, new byte[]{1}, Bytes.toBytes(3L))
    ), client.<Void>newPromise()).get();

    Result[] results = client.get(TEST_TABLE, Arrays.asList(
        new Get(row1),
        new Get(row2),
        new Get(row3)
    ), client.<Result[]>newPromise()).get();
    assertEquals(3, results.length);
    assertArrayEquals(row1, results[0].getRow());
    assertArrayEquals(row2, results[1].getRow());
    assertArrayEquals(row3, results[2].getRow());
    assertEquals(1L, Bytes.toLong(CellUtil.cloneValue(results[0].getColumnLatestCell(DEFAULT_FAMILY, new byte[]{1}))));
    assertEquals(2L, Bytes.toLong(CellUtil.cloneValue(results[1].getColumnLatestCell(DEFAULT_FAMILY, new byte[]{1}))));
    assertEquals(3L, Bytes.toLong(CellUtil.cloneValue(results[2].getColumnLatestCell(DEFAULT_FAMILY, new byte[]{1}))));
    client.delete(TEST_TABLE, Arrays.asList(
        new Delete(row1),
        new Delete(row3)
    ), client.<Void>newPromise()).get();
    results = client.get(TEST_TABLE, Arrays.asList(
        new Get(row1),
        new Get(row2),
        new Get(row3)
    ), client.<Result[]>newPromise()).get();
    assertEquals(3, results.length);
    assertTrue(results[0].isEmpty());
    assertFalse(results[1].isEmpty());
    assertArrayEquals(row2, results[1].getRow());
    assertTrue(results[2].isEmpty());
  }

  @Test
  public void testCheckPut() throws Exception {
    byte[] row = new byte[]{8};

    Put put = new Put(row).add(DEFAULT_FAMILY, new byte[]{1}, new byte[]{2});
    client.put(TEST_TABLE, put, client.<Void>newPromise()).get();
    put = new Put(row).add(DEFAULT_FAMILY, new byte[]{1}, new byte[]{3});
    try {
      client.checkAndPut(TEST_TABLE, new byte[]{8, 2}, DEFAULT_FAMILY, new byte[]{1}, new byte[]{5}, put, client.<Boolean>newPromise()).get();
      fail();
    } catch (ExecutionException e) {}
    boolean checked = client.checkAndPut(TEST_TABLE, row, DEFAULT_FAMILY, new byte[]{1}, new byte[]{5}, put, client.<Boolean>newPromise()).get();
    assertFalse(checked);
    checked = client.checkAndPut(TEST_TABLE, row, DEFAULT_FAMILY, new byte[]{1}, new byte[]{2}, put, client.<Boolean>newPromise()).get();
    assertTrue(checked);
  }

  @Test
  public void testCheckDelete() throws Exception {
    byte[] row = new byte[]{9};

    Put put = new Put(row).add(DEFAULT_FAMILY, new byte[]{1}, new byte[]{2});
    client.put(TEST_TABLE, put, client.<Void>newPromise()).get();
    Delete delete = new Delete(row);
    try {
      client.checkAndDelete(TEST_TABLE, new byte[] { 9, 2 }, DEFAULT_FAMILY, new byte[] { 1 },
          new byte[] { 5 }, delete, client.<Boolean>newPromise()).get();
      fail();
    } catch (ExecutionException e) {}
    boolean checked = client.checkAndDelete(TEST_TABLE, row, DEFAULT_FAMILY, new byte[] { 1 },
        new byte[] { 5 }, delete, client.<Boolean>newPromise()).get();
    assertFalse(checked);
    checked = client.checkAndDelete(TEST_TABLE, row, DEFAULT_FAMILY, new byte[] { 1 },
        new byte[] { 2 }, delete, client.<Boolean>newPromise()).get();
    assertTrue(checked);
  }

  @Test
  public void testMutate() throws Exception {
    byte[] row = new byte[]{10};

    client.put(TEST_TABLE,
        new Put(row).add(DEFAULT_FAMILY, new byte[] { 1 }, new byte[] { 2 }),
        client.<Void>newPromise()).get();

    RowMutations mutations = new RowMutations(row);
    mutations.add(new Put(row).add(DEFAULT_FAMILY, new byte[]{2}, new byte[]{2}));
    mutations.add(new Delete(row).addColumn(DEFAULT_FAMILY, new byte[]{1}));
    client.mutateRow(TEST_TABLE, mutations, client.<Void>newPromise()).get();
    Result result = client.get(TEST_TABLE, new Get(row), client.<Result>newPromise()).get();
    assertNotNull(result);
    assertTrue(!result.isEmpty());
    assertArrayEquals(row, result.getRow());
    assertFalse(result.containsColumn(DEFAULT_FAMILY, new byte[]{1}));
    assertTrue(result.containsColumn(DEFAULT_FAMILY, new byte[]{2}));
    assertArrayEquals(new byte[] { 2 },
        CellUtil.cloneValue(result.getColumnLatestCell(DEFAULT_FAMILY, new byte[] { 2 })));
  }

  @Test
  public void testCheckMutate() throws Exception {
    byte[] row = new byte[]{11};

    client.put(TEST_TABLE,
        new Put(row).add(DEFAULT_FAMILY, new byte[] { 1 }, new byte[] { 2 }),
        client.<Void>newPromise()).get();

    RowMutations mutations = new RowMutations(row);
    mutations.add(new Put(row).add(DEFAULT_FAMILY, new byte[]{2}, new byte[]{2}));
    mutations.add(new Delete(row).addColumn(DEFAULT_FAMILY, new byte[]{1}));
    boolean checked = client.checkAndMutate(
        TEST_TABLE, row, DEFAULT_FAMILY, new byte[]{1},
        CompareFilter.CompareOp.EQUAL, new byte[]{3},
        mutations, client.<Boolean>newPromise()).get();
    assertFalse(checked);
    checked = client.checkAndMutate(
        TEST_TABLE, row, DEFAULT_FAMILY, new byte[]{1},
        CompareFilter.CompareOp.EQUAL, new byte[]{2},
        mutations, client.<Boolean>newPromise()).get();
    assertTrue(checked);
  }

  @Test
  public void testCoprocessor() throws Exception {
    byte[] row = new byte[]{12};

    final HBaseResponsePromise<Result> promise = client.newPromise();
    ClientProtos.ClientService.newStub(client.coprocessorService(TEST_TABLE, row)).get(
        client.getNewRpcController(promise),
        buildGetRequest(
            connection.getRegionLocator(TEST_TABLE).getRegionLocation(row, false)
                .getRegionInfo().getRegionName(),
            new Get(row)
        ),
        new RpcCallback<ClientProtos.GetResponse>() {
          @Override public void run(ClientProtos.GetResponse parameter) {
            promise.onSuccess(toResult(parameter.getResult()));
          }
        }
    );
    Result result = promise.get();
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testScanner() throws Exception {
    byte[] row1 = new byte[]{13, 1};
    byte[] row2 = new byte[]{13, 2};
    byte[] row3 = new byte[]{13, 3};

    client.put(TEST_TABLE, Arrays.asList(
        new Put(row1).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(1L)),
        new Put(row2).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(2L)),
        new Put(row3).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(3L))), client.<Void>newPromise()).get();

    Scan scan = new Scan();
    scan.setStartRow(new byte[]{13});
    scan.setStopRow(new byte[]{14});
    scan.setCaching(2);
    scan.setSmall(true);
    final AsyncResultScanner scanner = client.getScanner(TEST_TABLE, scan);
    scanner.nextBatch(new ResponseHandler<Result[]>() {
    	@Override public void onSuccess(Result[] results) {
    		assertFalse(scanner.isScanDone());
    		assertEquals(2, results.length);
    		assertArrayEquals(row1, results[0].getRow());
    		assertArrayEquals(row2, results[1].getRow());
    		return;
    	}

		@Override
		public void onFailure(IOException e) {}
    });
  }

  @Test
  public void testReverseScanner() throws Exception {
    byte[] row1 = new byte[]{14, 1};
    byte[] row2 = new byte[]{14, 2};
    byte[] row3 = new byte[]{14, 3};

    client.put(TEST_TABLE, Arrays.asList(
        new Put(row1).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(1L)),
        new Put(row2).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(2L)),
        new Put(row3).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(3L))), client.<Void>newPromise()).get();

    Scan scan = new Scan();
    scan.setStartRow(new byte[]{15});
    scan.setStopRow(new byte[]{14});
    scan.setReversed(true);
    scan.setSmall(true);
    scan.setCaching(2);
    try (AsyncResultScanner scanner = client.getScanner(TEST_TABLE, scan)) {
      Result[] results = scanner.nextBatch(client.<Result[]>newPromise()).get();
      assertFalse(scanner.isScanDone());
      assertEquals(2, results.length);
      assertArrayEquals(row3, results[0].getRow());
      assertArrayEquals(row2, results[1].getRow());
      results = scanner.nextBatch(client.<Result[]>newPromise()).get();
      assertTrue(scanner.isScanDone());
      assertEquals(1, results.length);
      assertArrayEquals(row1, results[0].getRow());
    }
  }

  @Test
  public void testSmallScanner() throws Exception {
    byte[] row1 = new byte[]{15, 1};
    byte[] row2 = new byte[]{15, 2};
    byte[] row3 = new byte[]{15, 3};

    client.put(TEST_TABLE, Arrays.asList(
        new Put(row1).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(1L)),
        new Put(row2).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(2L)),
        new Put(row3).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(3L))), client.<Void>newPromise()).get();
    Scan scan = new Scan();
    scan.setStartRow(new byte[]{15});
    scan.setStopRow(new byte[]{16});
    scan.setSmall(true);
    scan.setCaching(2);
    scan.setMaxResultSize(2);
    try (AsyncResultScanner scanner = client.getScanner(TEST_TABLE, scan)) {
      Result[] results = scanner.nextBatch(client.<Result[]>newPromise()).get();
      assertTrue(scanner.isScanDone());
      assertEquals(1, results.length);
      assertArrayEquals(row1, results[0].getRow());
    }
  }

  @Test
  public void testSmallReversedScanner() throws Exception {
    byte[] row1 = new byte[]{16, 1};
    byte[] row2 = new byte[]{16, 2};
    byte[] row3 = new byte[]{16, 3};

    client.put(TEST_TABLE, Arrays.asList(
        new Put(row1).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(1L)),
        new Put(row2).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(2L)),
        new Put(row3).add(DEFAULT_FAMILY, new byte[] { 1 }, Bytes.toBytes(3L))), client.<Void>newPromise()).get();

    Scan scan = new Scan();
    scan.setStartRow(new byte[]{17});
    scan.setStopRow(new byte[]{16});
    scan.setReversed(true);
    scan.setSmall(true);
    scan.setCaching(2);
    try (AsyncResultScanner scanner = client.getScanner(TEST_TABLE, scan)) {
      Result[] results = scanner.nextBatch(client.<Result[]>newPromise()).get();
      assertFalse(scanner.isScanDone());
      assertEquals(2, results.length);
      assertArrayEquals(row3, results[0].getRow());
      assertArrayEquals(row2, results[1].getRow());
      results = scanner.nextBatch(client.<Result[]>newPromise()).get();
      assertEquals(1, results.length);
      assertArrayEquals(row1, results[0].getRow());
      results = scanner.nextBatch(client.<Result[]>newPromise()).get();
      assertTrue(scanner.isScanDone());
      assertEquals(0, results.length);
    }
  }
}