package org.apache.hadoop.hbase.client;

/**
 * Abstract class necessary to access protected parameters
 */
public abstract class AbstractHbaseClient {
  protected final String clusterId;

  /**
   * Constructor
   *
   * @param connection to use for connection
   */
  public AbstractHbaseClient(HConnection connection) {
    this.clusterId = ((HConnectionManager.HConnectionImplementation) connection).clusterId;
  }
}