package mousio.hbase.async;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.AsyncPayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.AsyncRpcChannel;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hbase.protobuf.ProtobufUtil.toResult;
import static org.apache.hadoop.hbase.protobuf.RequestConverter.buildGetRequest;
import static org.apache.hadoop.hbase.protobuf.RequestConverter.buildMutateRequest;

/**
 * Hbase client.
 */
public class HbaseClient extends AbstractHbaseClient implements Closeable {
  private final AsyncRpcClient client;

  /**
   * Constructor
   *
   * @param connection to Hbase
   * @throws java.io.IOException if HConnection could not be set up
   */
  public HbaseClient(HConnection connection) throws IOException {
    super(connection);

    this.client = new AsyncRpcClient(connection, this.clusterId, null);
  }

  /**
   * Send a Get
   *
   * @param table   to run get on
   * @param get     to fetch
   * @param handler on response
   * @return the handler with the result
   */
  public <H extends ResponseHandler<Result>> H get(TableName table, Get get, final H handler) {
    try {
      HRegionLocation location = this.client.getRegionLocation(table, get.getRow(), false);

      client.getClientService(location).get(
          client.newRpcController(handler),
          buildGetRequest(
              location.getRegionInfo().getRegionName(),
              get
          ),
          new RpcCallback<ClientProtos.GetResponse>() {
            @Override public void run(ClientProtos.GetResponse response) {
              handler.onSuccess(toResult(response.getResult()));
            }
          }
      );
    } catch (IOException e) {
      handler.onFailure(e);
    }
    return handler;
  }

  /**
   * Send a Get
   *
   * @param table   to run get on
   * @param gets    to fetch
   * @param handler on response
   * @return the handler with the result
   */
  public <H extends ResponseHandler<Result[]>> H get(TableName table, List<Get> gets, final H handler) {
    final Result[] results = new Result[gets.size()];
    final AtomicInteger counter = new AtomicInteger(0);
    final AtomicBoolean complete = new AtomicBoolean(false);

    for (int i = 0; i < gets.size(); i++) {
      this.get(table, gets.get(i), new ResultListener<Result>(i) {
        @Override public void onSuccess(Result response) {
          if (!complete.get()) {
            synchronized (results) {
              results[this.index] = response;
              if (counter.incrementAndGet() == results.length) {
                handler.onSuccess(results);
              }
            }
          }
        }

        @Override public void onFailure(IOException e) {
          if (!complete.get()) {
            handler.onFailure(e);
            complete.set(true);
          }
        }
      });
    }

    return handler;
  }

  /**
   * Send a scan and get a cell scanner
   *
   * @param table to get scanner from
   * @param scan  to perform
   * @return the handler with the ResponseHandler if successful
   */
  public AsyncResultScanner getScanner(TableName table, Scan scan) {
    if (scan.isReversed()) {
      if (scan.isSmall()) {
        return new AsyncClientSmallReversedScanner(this.client, scan, table);
      } else {
        return new AsyncReversedClientScanner(this.client, scan, table);
      }
    }

    if (scan.isSmall()) {
      return new AsyncClientSmallScanner(this.client, scan, table);
    } else {
      return new AsyncClientScanner(this.client, scan, table);
    }
  }

  /**
   * Send a put
   *
   * @param table   to send Put to
   * @param put     to send
   * @param handler to handle exceptions
   * @param <H>     Type of Handler
   * @return handler
   */

  public <H extends ResponseHandler<Void>> H put(TableName table, Put put, final H handler) {
    try {
      HRegionLocation location = this.client.getRegionLocation(table, put.getRow(), false);

      client.getClientService(location).mutate(
          client.newRpcController(handler),
          buildMutateRequest(
              location.getRegionInfo().getRegionName(),
              put
          ),
          new RpcCallback<ClientProtos.MutateResponse>() {
            @Override public void run(ClientProtos.MutateResponse response) {
              handler.onSuccess(null);
            }
          }
      );
    } catch (IOException e) {
      handler.onFailure(e);
    }
    return handler;
  }

  /**
   * Send a list of puts to the server
   *
   * @param table   to send puts to
   * @param puts    to send
   * @param handler to handle exceptions
   * @param <H>     Handler to handle any exceptions
   * @return handler
   */
  public <H extends ResponseHandler<Void>> H put(TableName table, List<Put> puts, final H handler) {
    final int size = puts.size();
    final AtomicInteger counter = new AtomicInteger(0);

    for (Put put : puts) {
      this.put(table, put, new ResponseHandler<Void>() {
        @Override public void onSuccess(Void response) {
          if (counter.incrementAndGet() == size) {
            handler.onSuccess(null);
          }
        }

        @Override public void onFailure(IOException e) {
          if (counter.get() < size) {
            handler.onFailure(e);
            counter.set(size);
          }
        }
      });
    }
    return handler;
  }


  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the put.  If the passed value is null, the check
   * is for the lack of column (ie: non-existance)
   *
   * @param table     to check on and send put to
   * @param row       to check
   * @param family    column family to check
   * @param qualifier column qualifier to check
   * @param value     the expected value
   * @param put       data to put if check succeeds
   * @param handler   to handle exceptions
   * @param <H>       Handler to handle any exceptions
   * @return true if the new put was executed, false otherwise
   */
  public <H extends ResponseHandler<Boolean>> H checkAndPut(TableName table, byte[] row, byte[] family, byte[] qualifier,
                                                            byte[] value, Put put, final H handler) {
    try {
      HRegionLocation location = this.client.getRegionLocation(table, put.getRow(), false);

      client.getClientService(location).mutate(
          client.newRpcController(handler),
          buildMutateRequest(
              location.getRegionInfo().getRegionName(),
              row,
              family,
              qualifier,
              new BinaryComparator(value),
              HBaseProtos.CompareType.EQUAL,
              put
          ),
          new RpcCallback<ClientProtos.MutateResponse>() {
            @Override public void run(ClientProtos.MutateResponse response) {
              handler.onSuccess(response.getProcessed());
            }
          }
      );
    } catch (IOException e) {
      handler.onFailure(e);
    }
    return handler;
  }

  /**
   * Deletes the specified cells/row.
   *
   * @param table   to send delete to
   * @param delete  The object that specifies what to delete.
   * @param handler to handle exceptions
   * @param <H>     Handler to handle any exceptions
   * @return handler of responses and exceptions
   */
  public <H extends ResponseHandler<Void>> H delete(TableName table, Delete delete, final H handler) {
    try {
      HRegionLocation location = this.client.getRegionLocation(table, delete.getRow(), false);

      client.getClientService(location).mutate(
          client.newRpcController(handler),
          buildMutateRequest(
              location.getRegionInfo().getRegionName(),
              delete
          ),
          new RpcCallback<ClientProtos.MutateResponse>() {
            @Override public void run(ClientProtos.MutateResponse response) {
              handler.onSuccess(null);
            }
          }
      );
    } catch (IOException e) {
      handler.onFailure(e);
    }
    return handler;
  }

  /**
   * Deletes the specified cells/rows in bulk.
   *
   * @param table   to send deletes to
   * @param deletes List of things to delete.  List gets modified by this
   *                method (in particular it gets re-ordered, so the order in which the elements
   *                are inserted in the list gives no guarantee as to the order in which the
   *                {@link Delete}s are executed).
   * @param handler to handle exceptions
   * @param <H>     Handler to handle any exceptions
   * @return handler of responses and exceptions
   */
  public <H extends ResponseHandler<Void>> H delete(TableName table, List<Delete> deletes, final H handler) {
    final int size = deletes.size();
    final AtomicInteger counter = new AtomicInteger(0);

    for (Delete delete : deletes) {
      this.delete(table, delete, new ResponseHandler<Void>() {
        @Override public void onSuccess(Void response) {
          if (counter.incrementAndGet() == size) {
            handler.onSuccess(null);
          }
        }

        @Override public void onFailure(IOException e) {
          if (counter.get() < size) {
            handler.onFailure(e);
            counter.set(size);
          }
        }
      });
    }
    return handler;
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected
   * value. If it does, it adds the delete.  If the passed value is null, the
   * check is for the lack of column (ie: non-existance)
   *
   * @param table     to send check and delete to
   * @param row       to check
   * @param family    column family to check
   * @param qualifier column qualifier to check
   * @param value     the expected value
   * @param delete    data to delete if check succeeds
   * @param handler   to handle exceptions
   * @param <H>       Handler to handle any exceptions
   * @return true if the new delete was executed, false otherwise
   */
  public <H extends ResponseHandler<Boolean>> H checkAndDelete(TableName table, byte[] row, byte[] family, byte[] qualifier,
                                                               byte[] value, Delete delete, final H handler) {
    try {
      HRegionLocation location = this.client.getRegionLocation(table, delete.getRow(), false);

      client.getClientService(location).mutate(
          client.newRpcController(handler),
          buildMutateRequest(
              location.getRegionInfo().getRegionName(),
              row,
              family,
              qualifier,
              new BinaryComparator(value),
              HBaseProtos.CompareType.EQUAL,
              delete
          ),
          new RpcCallback<ClientProtos.MutateResponse>() {
            @Override public void run(ClientProtos.MutateResponse response) {
              handler.onSuccess(response.getProcessed());
            }
          }
      );
    } catch (IOException e) {
      handler.onFailure(e);
    }
    return handler;
  }

  /**
   * Performs multiple mutations atomically on a single row. Currently
   * {@link Put} and {@link Delete} are supported.
   *
   * @param table   to mutate row on
   * @param rm      object that specifies the set of mutations to perform atomically
   * @param handler to handle exceptions
   * @param <H>     Handler to handle any exceptions
   * @return response handler
   */
  public <H extends ResponseHandler<Void>> H mutateRow(TableName table, final RowMutations rm, final H handler) {
    try {
      HRegionLocation location = this.client.getRegionLocation(table, rm.getRow(), false);

      ClientProtos.RegionAction.Builder regionMutationBuilder = RequestConverter.buildRegionAction(
          location.getRegionInfo().getRegionName(), rm);

      regionMutationBuilder.setAtomic(true);
      ClientProtos.MultiRequest request =
          ClientProtos.MultiRequest.newBuilder().addRegionAction(regionMutationBuilder.build()).build();

      client.getClientService(location).multi(
          client.newRpcController(handler),
          request,
          new RpcCallback<ClientProtos.MultiResponse>() {
            @Override public void run(ClientProtos.MultiResponse response) {
              handler.onSuccess(null);
            }
          }
      );
    } catch (IOException e) {
      handler.onFailure(e);
    }
    return handler;
  }

  /**
   * Appends values to one or more columns within a single row.
   * <p/>
   * This operation does not appear atomic to readers.  Appends are done
   * under a single row lock, so write operations to a row are synchronized, but
   * readers do not take row locks so get and scan operations can see this
   * operation partially completed.
   *
   * @param table   to append to
   * @param append  object that specifies the columns and amounts to be used
   *                for the increment operations
   * @param handler to handle exceptions
   * @param <H>     Handler to handle any exceptions
   * @return handler with Result on success
   */
  public <H extends ResponseHandler<Result>> H append(TableName table, final Append append, final H handler) {
    try {
      HRegionLocation location = this.client.getRegionLocation(table, append.getRow(), false);

      NonceGenerator ng = this.client.getNonceGenerator();
      final long nonceGroup = ng.getNonceGroup(), nonce = ng.newNonce();
      final AsyncPayloadCarryingRpcController controller = client.newRpcController(handler);

      client.getClientService(location).mutate(
          controller,
          buildMutateRequest(
              location.getRegionInfo().getRegionName(),
              append,
              nonceGroup,
              nonce
          ),
          new RpcCallback<ClientProtos.MutateResponse>() {
            @Override public void run(ClientProtos.MutateResponse response) {
              try {
                handler.onSuccess(ProtobufUtil.toResult(response.getResult(), controller.cellScanner()));
              } catch (IOException e) {
                handler.onFailure(e);
              }
            }
          }
      );
    } catch (IOException e) {
      handler.onFailure(e);
    }
    return handler;
  }

  /**
   * Increments one or more columns within a single row.
   * <p/>
   * This operation does not appear atomic to readers.  Increments are done
   * under a single row lock, so write operations to a row are synchronized, but
   * readers do not take row locks so get and scan operations can see this
   * operation partially completed.
   *
   * @param table     to increment on
   * @param increment object that specifies the columns and amounts to be used
   *                  for the increment operations
   * @param handler   to handle exceptions
   * @param <H>       Handler to handle any exceptions
   * @return handler with on success the values of columns after the increment
   */
  public <H extends ResponseHandler<Result>> H increment(TableName table, final Increment increment, final H handler) {
    try {
      HRegionLocation location = this.client.getRegionLocation(table, increment.getRow(), false);

      NonceGenerator ng = this.client.getNonceGenerator();
      final long nonceGroup = ng.getNonceGroup(), nonce = ng.newNonce();
      final AsyncPayloadCarryingRpcController controller = client.newRpcController(handler);

      client.getClientService(location).mutate(
          controller,
          buildMutateRequest(
              location.getRegionInfo().getRegionName(),
              increment,
              nonceGroup,
              nonce
          ),
          new RpcCallback<ClientProtos.MutateResponse>() {
            @Override public void run(ClientProtos.MutateResponse response) {
              try {
                handler.onSuccess(ProtobufUtil.toResult(response.getResult(), controller.cellScanner()));
              } catch (IOException e) {
                handler.onFailure(e);
              }
            }
          }
      );
    } catch (IOException e) {
      handler.onFailure(e);
    }
    return handler;
  }

  /**
   * See {@link #incrementColumnValue(TableName, byte[], byte[], byte[], long, Durability, ResponseHandler)}
   * <p/>
   * The {@link Durability} is defaulted to {@link Durability#SYNC_WAL}.
   *
   * @param table     to increment column value on
   * @param row       The row that contains the cell to increment.
   * @param family    The column family of the cell to increment.
   * @param qualifier The column qualifier of the cell to increment.
   * @param amount    The amount to increment the cell with (or decrement, if the
   *                  amount is negative).
   * @param handler   to handle response
   * @param <H>       Handler to handle any exceptions
   * @return Response handler: on success The new value, post increment.
   */
  public <H extends ResponseHandler<Long>> H incrementColumnValue(TableName table, byte[] row, byte[] family, byte[] qualifier,
                                                                  long amount, final H handler) {
    return incrementColumnValue(table, row, family, qualifier, amount, Durability.SYNC_WAL, handler);
  }

  /**
   * Atomically increments a column value. If the column value already exists
   * and is not a big-endian long, this could throw an exception. If the column
   * value does not yet exist it is initialized to <code>amount</code> and
   * written to the specified column.
   * <p/>
   * <p>Setting durability to {@link Durability#SKIP_WAL} means that in a fail
   * scenario you will lose any increments that have not been flushed.
   *
   * @param table      to increment column value on
   * @param row        The row that contains the cell to increment.
   * @param family     The column family of the cell to increment.
   * @param qualifier  The column qualifier of the cell to increment.
   * @param amount     The amount to increment the cell with (or decrement, if the
   *                   amount is negative).
   * @param durability The persistence guarantee for this increment.
   * @param handler    to handle response
   * @param <H>        Handler to handle any exceptions
   * @return Response handler: on success The new value, post increment.
   */
  public <H extends ResponseHandler<Long>> H incrementColumnValue(TableName table, byte[] row, final byte[] family, final byte[] qualifier,
                                                                  long amount, Durability durability, final H handler) {
    NullPointerException npe = null;
    if (row == null) {
      npe = new NullPointerException("row is null");
    } else if (family == null) {
      npe = new NullPointerException("family is null");
    } else if (qualifier == null) {
      npe = new NullPointerException("qualifier is null");
    }
    if (npe != null) {
      handler.onFailure(new IOException(
          "Invalid arguments to incrementColumnValue", npe));
    } else {
      try {
        HRegionLocation location = this.client.getRegionLocation(table, row, false);

        NonceGenerator ng = this.client.getNonceGenerator();
        final long nonceGroup = ng.getNonceGroup(), nonce = ng.newNonce();

        ClientProtos.MutateRequest request = RequestConverter.buildIncrementRequest(
            location.getRegionInfo().getRegionName(), row, family,
            qualifier, amount, durability, nonceGroup, nonce);

        final AsyncPayloadCarryingRpcController controller = client.newRpcController(handler);

        client.getClientService(location).mutate(
            controller,
            request,
            new RpcCallback<ClientProtos.MutateResponse>() {
              @Override public void run(ClientProtos.MutateResponse response) {
                try {
                  Result result = ProtobufUtil.toResult(response.getResult(), controller.cellScanner());
                  handler.onSuccess(Bytes.toLong(result.getValue(family, qualifier)));
                } catch (IOException e) {
                  handler.onFailure(e);
                }
              }
            }
        );
      } catch (IOException e) {
        handler.onFailure(e);
      }
    }

    return handler;
  }

  /**
   * Atomically checks if a row/family/qualifier value matches the expected val
   * If it does, it performs the row mutations.  If the passed value is null, t
   * is for the lack of column (ie: non-existence)
   *
   * @param table     to check and mutate
   * @param row       to check
   * @param family    column family to check
   * @param qualifier column qualifier to check
   * @param compareOp the comparison operator
   * @param value     the expected value
   * @param mutation  mutations to perform if check succeeds
   * @param handler   for the response
   * @param <H>       Handler to handle any exceptions
   * @return handler with response: true if the new put was executed, false otherwise
   */
  public <H extends ResponseHandler<Boolean>> H checkAndMutate(TableName table, byte[] row, byte[] family, byte[] qualifier, CompareFilter.CompareOp compareOp,
                                                               byte[] value, RowMutations mutation, final H handler) {
    try {
      HRegionLocation location = this.client.getRegionLocation(table, mutation.getRow(), false);

      client.getClientService(location).multi(
          client.newRpcController(handler),
          buildMutateRequest(
              location.getRegionInfo().getRegionName(),
              row,
              family,
              qualifier,
              new BinaryComparator(value),
              HBaseProtos.CompareType.valueOf(compareOp.name()),
              mutation
          ),
          new RpcCallback<ClientProtos.MultiResponse>() {
            @Override public void run(ClientProtos.MultiResponse response) {
              handler.onSuccess(response.getProcessed());
            }
          }
      );
    } catch (IOException e) {
      handler.onFailure(e);
    }
    return handler;
  }


  /**
   * Creates and returns a {@link com.google.protobuf.RpcChannel} instance connected to the
   * table region containing the specified row.  The row given does not actually have
   * to exist.  Whichever region would contain the row based on start and end keys will
   * be used.  Note that the {@code row} parameter is also not passed to the
   * coprocessor handler registered for this protocol, unless the {@code row}
   * is separately passed as an argument in the service request.  The parameter
   * here is only used to locate the region used to handle the call.
   * <p/>
   * <p>
   * The obtained {@link com.google.protobuf.RpcChannel} instance can be used to access a published
   * coprocessor {@link com.google.protobuf.Service} using standard protobuf service invocations:
   * </p>
   * <p/>
   * <div style="background-color: #cccccc; padding: 2px">
   * <blockquote><pre>
   * CoprocessorRpcChannel channel = myTable.coprocessorService(rowkey);
   * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
   * MyCallRequest request = MyCallRequest.newBuilder()
   *     ...
   *     .build();
   * MyCallResponse response = service.myCall(null, request);
   * </pre></blockquote></div>
   *
   * @param table to get service from
   * @param row   The row key used to identify the remote region location
   * @return A CoprocessorRpcChannel instance
   * @throws java.io.IOException when there was an error creating connection or getting location
   */
  public AsyncRpcChannel coprocessorService(TableName table, byte[] row) throws IOException {
    HRegionLocation location = this.client.getRegionLocation(table, row, false);

    return client.getConnection(
        ClientProtos.ClientService.getDescriptor(),
        location
    );
  }

  /**
   * Get a new promise chained to event loop of internal netty client
   *
   * @param <T> Type of response to return
   * @return Hbase Response Promise
   */
  public <T> HbaseResponsePromise<T> newPromise() {
    return new HbaseResponsePromise<>(client.getEventLoop());
  }

  @Override public void close() throws IOException {
    client.close();
  }

  /**
   * Get a new Rpc controller
   *
   * @param promise to handle result
   * @return new RpcController
   */
  public RpcController newRpcController(HbaseResponsePromise<?> promise) {
    return this.client.newRpcController(promise);
  }

  /**
   * Listens for results with an index
   *
   * @param <R> Type of result listened to
   */
  private abstract class ResultListener<R> implements ResponseHandler<R> {
    protected int index;

    /**
     * Constructor
     *
     * @param i index for result
     */
    public ResultListener(int i) {
      this.index = i;
    }
  }
}