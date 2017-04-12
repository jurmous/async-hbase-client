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

import shaded.hbase.com.google.protobuf.ServiceException;
import shaded.hbase.common.io.netty.util.Timeout;
import shaded.hbase.common.io.netty.util.TimerTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ipc.AsyncRpcClient;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Runs an rpc'ing {@link AsyncRetryingCallable}. Sets into rpc client
 * threadlocal outstanding timeouts as so we don't persist too much.
 * Dynamic rather than static so can set the generic appropriately.
 *
 * @param <T> Type of Response
 */
public class AsyncRpcRetryingCaller<T> {
  static final Log LOG = LogFactory.getLog(AsyncRpcRetryingCaller.class);
  /**
   * Timeout for the call including retries
   */
  private int callTimeout;
  /**
   * When we started making calls.
   */
  private long globalStartTime;
  /**
   * Start and end times for a single call.
   */
  private final static int MIN_RPC_TIMEOUT = 2000;

  private final long pause;
  private final int retries;

  /**
   * Constructor
   *
   * @param pause   before retry
   * @param retries max amount of retries
   */
  public AsyncRpcRetryingCaller(long pause, int retries) {
    this.pause = pause;
    this.retries = retries;
  }

  /**
   * Retries if invocation fails.
   *
   * @param callable to run
   * @param handler  to handle response
   */
  public synchronized void callWithRetries(AsyncRetryingCallable<T> callable, ResponseHandler<T> handler) {
    callWithRetries(callable, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT, handler);
  }

  /**
   * Retries if invocation fails.
   *
   * @param callTimeout Timeout for this call
   * @param handler     to handle response
   * @param callable    The {@link AsyncRetryingCallable} to run.
   */
  @edu.umd.cs.findbugs.annotations.SuppressWarnings
      (value = "SWL_SLEEP_WITH_LOCK_HELD", justification = "na")
  public synchronized void callWithRetries(final AsyncRetryingCallable<T> callable, int callTimeout, final ResponseHandler<T> handler) {
    this.callTimeout = callTimeout;
    this.globalStartTime = EnvironmentEdgeManager.currentTime();

    try {
      callable.prepare(true); // if called with false, check table status on ZK
    } catch (IOException e) {
      handler.onFailure(e);
      return;
    }
    callable.call(new CallResponseHandler<>(handler, callable));
  }

  /**
   * @param expectedSleep expected sleep time
   * @return Calculate how long a single call took
   */
  private long singleCallDuration(final long expectedSleep) {
    return (EnvironmentEdgeManager.currentTime() - this.globalStartTime)
        + MIN_RPC_TIMEOUT + expectedSleep;
  }

  /**
   * Call the server once only.
   * {@link AsyncRetryingCallable} has a strange shape so we can do retries.  Use this invocation if you
   * want to do a single call only (A call to {@link AsyncRetryingCallable#call(ResponseHandler)} will not likely
   * succeed).
   *
   * @param callable    to call
   * @param callTimeout to timeout call
   * @param handler     for response
   */
  public void callWithoutRetries(AsyncRetryingCallable<T> callable, int callTimeout, final ResponseHandler<T> handler) {
    // The code of this method should be shared with withRetries.
    this.globalStartTime = EnvironmentEdgeManager.currentTime();
    this.callTimeout = callTimeout;
    try {
      callable.prepare(true); // if called with false, check table status on ZK
    } catch (IOException e) {
      handler.onFailure(e);
      return;
    }
    callable.call(new ResponseHandler<T>() {
      @Override public void onSuccess(T response) {
        handler.onSuccess(response);
      }

      @Override public void onFailure(IOException e) {
        try {
          Throwable t2 = translateException(e);
          ExceptionUtil.rethrowIfInterrupt(t2);

          // It would be nice to clear the location cache here.
          if (t2 instanceof IOException) {
            handler.onFailure((IOException) t2);
          } else {
            throw new RuntimeException(t2);
          }
        } catch (InterruptedIOException | DoNotRetryIOException e2) {
          handler.onFailure(e2);
        }
      }
    });
  }

  /**
   * Get the good or the remote exception if any, throws the DoNotRetryIOException.
   *
   * @param t the throwable to analyze
   * @return the translated exception, if it's not a DoNotRetryIOException
   * @throws DoNotRetryIOException - if we find it, we throw it instead of translating.
   */
  static Throwable translateException(Throwable t) throws DoNotRetryIOException {
    if (t instanceof UndeclaredThrowableException) {
      if (t.getCause() != null) {
        t = t.getCause();
      }
    }
    if (t instanceof RemoteException) {
      t = ((RemoteException) t).unwrapRemoteException();
    }
    if (t instanceof LinkageError) {
      throw new DoNotRetryIOException(t);
    }
    if (t instanceof ServiceException) {
      ServiceException se = (ServiceException) t;
      Throwable cause = se.getCause();
      if (cause != null && cause instanceof DoNotRetryIOException) {
        throw (DoNotRetryIOException) cause;
      }
      // Don't let ServiceException out; its rpc specific.
      t = cause;
      // t could be a RemoteException so go around again.
      t = translateException(t);
    } else if (t instanceof DoNotRetryIOException) {
      throw (DoNotRetryIOException) t;
    }
    return t;
  }

  /**
   * Response handler
   *
   * @param <T> Type of response
   */
  private class CallResponseHandler<T> implements ResponseHandler<T> {
    private final ResponseHandler<T> handler;
    private final AsyncRetryingCallable<T> callable;
    private final List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions = new ArrayList<>();
    long expectedSleep = 0;
    int tries = 0;

    /**
     * Constructor
     *
     * @param handler  which handles the response
     * @param callable to call for request
     */
    public CallResponseHandler(ResponseHandler<T> handler, AsyncRetryingCallable<T> callable) {
      this.handler = handler;
      this.callable = callable;
    }

    @Override public void onSuccess(T response) {
      handler.onSuccess(response);
    }

    @Override public void onFailure(IOException e) {
      try {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Call exception, tries=" + tries + ", retries=" + retries + ", retryTime=" +
              (EnvironmentEdgeManager.currentTime() - globalStartTime) + "ms", e);
        }
        // translateException throws exception when should not retry: i.e. when request is bad.
        Throwable t = translateException(e);
        callable.throwable(t, retries != 1);
        RetriesExhaustedException.ThrowableWithExtraContext qt =
            new RetriesExhaustedException.ThrowableWithExtraContext(t,
                EnvironmentEdgeManager.currentTime(), toString());
        exceptions.add(qt);
        ExceptionUtil.rethrowIfInterrupt(t);
        if (tries >= retries - 1) {
          throw new RetriesExhaustedException(tries, exceptions);
        }
        // If the server is dead, we need to wait a little before retrying, to give
        //  a chance to the regions to be
        // tries hasn't been bumped up yet so we use "tries + 1" to get right pause time
        expectedSleep = callable.sleep(pause, tries + 1);

        // If, after the planned sleep, there won't be enough time left, we stop now.
        long duration = singleCallDuration(expectedSleep);
        if (duration > callTimeout) {
          String msg = "callTimeout=" + callTimeout + ", callDuration=" + duration +
              ": " + callable.getExceptionMessageAdditionalDetail();
          throw (SocketTimeoutException) (new SocketTimeoutException(msg).initCause(t));
        }
      } catch (IOException io) {
        handler.onFailure(io);
        return;
      }

      // Wait till next retry
      AsyncRpcClient.WHEEL_TIMER.newTimeout(new TimerTask() {
        @Override public void run(Timeout timeout) throws Exception {
          try {
            callable.prepare(false); // if called with false, check table status on ZK
          } catch (IOException e2) {
            handler.onFailure(e2);
            return;
          }
          callable.call(CallResponseHandler.this);
        }
      }, expectedSleep, TimeUnit.MILLISECONDS);
    }
  }
}