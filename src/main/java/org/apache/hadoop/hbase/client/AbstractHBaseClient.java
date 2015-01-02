package org.apache.hadoop.hbase.client;

/**
 * Abstract class necessary to access protected parameters
 */
public abstract class AbstractHBaseClient {
  protected final String clusterId;
  protected final ClusterConnection connection;

  /**
   * Constructor
   *
   * @param connection to use for connection
   */
  public AbstractHBaseClient(ClusterConnection connection) {
    this.connection = connection;
    this.clusterId = ((ConnectionManager.HConnectionImplementation) connection).clusterId;
  }

  /**
   * Get HConnection to talk to master/cluster
   *
   * @return HConnection
   */
  public Connection getConnection() {
    return connection;
  }
}