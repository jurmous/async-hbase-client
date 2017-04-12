package mousio.hbase.async;

import shaded.hbase.common.io.netty.util.concurrent.DefaultPromise;
import shaded.hbase.common.io.netty.util.concurrent.EventExecutor;
import org.apache.hadoop.hbase.client.ResponseHandler;

import java.io.IOException;

/**
 * Hbase response promise
 *
 * @param <T> Type of response promised
 */
public class HBaseResponsePromise<T> extends DefaultPromise<T> implements ResponseHandler<T> {
  /**
   * Constructor
   *
   * @param executor for promise
   */
  public HBaseResponsePromise(EventExecutor executor) {
    super(executor);
  }

  @Override public void onSuccess(T response) {
    super.setSuccess(response);
  }

  @Override public void onFailure(IOException e) {
    super.setFailure(e);
  }
}