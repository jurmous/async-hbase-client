package org.apache.hadoop.hbase.client;

/**
 * Abstract class necessary to access protected parameters
 */
public abstract class AbstractHBaseClient {
  protected final String clusterId;
  private final HConnection connection;

  /**
   * Constructor
   *
   * @param connection to use for connection
   */
  public AbstractHBaseClient(HConnection connection) {
    this.connection = connection;
    this.clusterId = ((HConnectionManager.HConnectionImplementation) connection).clusterId;
  }

  /**
   * Get HConnection to talk to master/cluster
   *
   * @return HConnection
   */
  public HConnection getConnection() {
    return connection;
  }
}