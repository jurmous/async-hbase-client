package org.apache.hadoop.hbase.client;

import java.io.IOException;

/**
 * Interface for async responses
 *
 * @param <T> Type of response
 */
public interface ResponseHandler<T> {
  /**
   * Encountered success
   *
   * @param response on success
   */
  public void onSuccess(T response);

  /**
   * Encountered failure
   *
   * @param e describing error
   */
  public void onFailure(IOException e);
}