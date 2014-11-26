package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

import java.io.Closeable;

/**
 * Interface for client-side scanning.
 * Go to {@link HTable} to obtain instances.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface AsyncResultScanner extends Closeable {

  /**
   * Get the default number of rows as set by "hbase.client.scanner.caching"
   *
   * @param handler to handle responses
   * @return Between zero and <param>nbRows</param> Results
   */
  <H extends ResponseHandler<Result[]>> H nextBatch(H handler);

  /**
   * Check if the scanner is done
   *
   * @return true if the scanner is done scanning all rows
   */
  boolean isScanDone();
}