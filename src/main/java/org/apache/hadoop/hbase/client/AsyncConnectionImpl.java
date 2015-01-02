package org.apache.hadoop.hbase.client;

import java.io.IOException;

    import org.apache.hadoop.conf.Configuration;
    import org.apache.hadoop.hbase.ipc.RpcClient;
    import org.apache.hadoop.hbase.security.User;

    import java.io.IOException;
    import java.util.concurrent.ExecutorService;

/**
 * Async implementation of the HConnectionImpl
 */
public class AsyncConnectionImpl extends ConnectionManager.HConnectionImplementation {
  AsyncConnectionImpl(Configuration conf, boolean managed) throws IOException {
    super(conf, managed);
  }

  AsyncConnectionImpl(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws IOException {
    super(conf, managed, pool, user);
  }

  protected AsyncConnectionImpl(Configuration conf) {
    super(conf);
  }

  @Override public RpcClient getRpcClient() {
    return super.getRpcClient();
  }
}