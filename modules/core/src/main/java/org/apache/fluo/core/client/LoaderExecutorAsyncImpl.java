/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.core.client;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.MoreExecutors;
import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.core.async.AsyncCommitObserver;
import org.apache.fluo.core.async.AsyncTransaction;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.TransactionImpl;
import org.apache.fluo.core.log.TracingTransaction;
import org.apache.fluo.core.util.Counter;
import org.apache.fluo.core.util.FluoExecutors;
import org.slf4j.LoggerFactory;

public class LoaderExecutorAsyncImpl implements LoaderExecutor {
  private final ExecutorService executor;
  private final Semaphore semaphore;
  private final int semaphoreSize;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private final AtomicReference<Throwable> exceptionRef = new AtomicReference<>(null);
  private final Environment env;

  private final Counter commiting = new Counter();

  private void setException(Throwable t) {
    if (!exceptionRef.compareAndSet(null, t)) {
      LoggerFactory.getLogger(LoaderExecutorAsyncImpl.class)
          .debug("Multiple exceptions occured, not reporting subsequent ones", t);
    }
  }

  class LoaderCommitObserver<T extends Loader> implements AsyncCommitObserver, Runnable {

    AsyncTransaction txi;
    T loader;
    private AtomicBoolean done = new AtomicBoolean(false);
    private String identity;
    private CompletableFuture<T> future;

    private void close() {
      txi = null;
      if (done.compareAndSet(false, true)) {
        commiting.decrement();
      } else {
        // its only expected that this should be called once.. if its called multiple times in
        // indicates an error in async code
        LoggerFactory.getLogger(LoaderCommitObserver.class).error("Close called twice ",
            new Exception());
      }
    }


    public LoaderCommitObserver(String alias, T loader2) {
      this.identity = alias;
      this.loader = loader2;
    }


    public LoaderCommitObserver(String alias, T loader2, CompletableFuture<T> future) {
      this(alias, loader2);
      this.future = future;
    }


    @Override
    public void committed() {
      close();
      if (future != null) {
        future.complete(loader);
      }
    }

    @Override
    public void failed(Throwable t) {
      close();
      if (future == null) {
        setException(t);
      } else {
        future.completeExceptionally(t);
      }
    }

    @Override
    public void alreadyAcknowledged() {
      close();
      // should not happen
      LoggerFactory.getLogger(LoaderCommitObserver.class).error("Already ack called for loader ",
          new Exception());
    }

    @Override
    public void commitFailed(String msg) {
      txi = null;
      // retry transaction
      executor.submit(this);
    }

    @Override
    public void run() {
      txi = new TransactionImpl(env);

      if (TracingTransaction.isTracingEnabled()) {
        txi = new TracingTransaction(txi, loader.getClass(), identity);
      }

      Loader.Context context = new Loader.Context() {
        @Override
        public SimpleConfiguration getAppConfiguration() {
          return env.getAppConfiguration();
        }

        @Override
        public MetricsReporter getMetricsReporter() {
          return env.getMetricsReporter();
        }
      };

      try {
        loader.load(txi, context);
        env.getSharedResources().getCommitManager().beginCommit(txi, identity, this);
      } catch (Exception e) {
        if (future == null) {
          setException(e);
        } else {
          future.completeExceptionally(e);
        }
        close();
        LoggerFactory.getLogger(LoaderCommitObserver.class).debug(e.getMessage(), e);
      }
    }
  }

  private class QueueReleaseRunnable implements Runnable {

    LoaderCommitObserver loaderTask;

    QueueReleaseRunnable(LoaderCommitObserver loaderTask) {
      this.loaderTask = loaderTask;
    }

    @Override
    public void run() {
      // was just taken out of queue, release semaphore
      semaphore.release();
      loaderTask.run();
    }

  }

  public LoaderExecutorAsyncImpl(FluoConfiguration config, Environment env) {
    this(config, config.getLoaderThreads(), config.getLoaderQueueSize(), env);
  }

  private LoaderExecutorAsyncImpl(FluoConfiguration config, int numThreads, int queueSize,
      Environment env) {

    if (numThreads < 0 || (numThreads == 0 && queueSize != 0)) {
      throw new IllegalArgumentException(
          "numThreads must be positive OR numThreads and queueSize must both be 0");
    }

    if (queueSize < 0 || (numThreads != 0 && queueSize == 0)) {
      throw new IllegalArgumentException(
          "queueSize must be non-negative OR numThreads and queueSize must both be 0");
    }

    this.env = env;
    this.semaphoreSize = queueSize == 0 ? 1 : queueSize;
    this.semaphore = new Semaphore(semaphoreSize);
    if (numThreads == 0) {
      this.executor = MoreExecutors.newDirectExecutorService();
    } else {
      this.executor = FluoExecutors.newFixedThreadPool(numThreads, "loader");
    }
  }

  @Override
  public void execute(Loader loader) {
    execute(loader.getClass().getSimpleName(), loader);
  }

  @Override
  public void execute(String alias, Loader loader) {
    if (exceptionRef.get() != null) {
      throw new RuntimeException("Previous failure", exceptionRef.get());
    }

    try {
      while (!semaphore.tryAcquire(50, TimeUnit.MILLISECONDS)) {
        if (closed.get()) {
          throw new IllegalStateException("LoaderExecutor is closed");
        }
      }
    } catch (InterruptedException e1) {
      throw new RuntimeException(e1);
    }

    try {
      commiting.increment();
      executor.execute(new QueueReleaseRunnable(new LoaderCommitObserver(alias, loader)));
    } catch (RejectedExecutionException rje) {
      semaphore.release();
      commiting.decrement();
      throw rje;
    }
  }

  @Override
  public <T extends Loader> CompletableFuture<T> submit(T loader) {
    return submit(loader.getClass().getSimpleName(), loader);
  }

  @Override
  public <T extends Loader> CompletableFuture<T> submit(String alias, T loader) {
    CompletableFuture<T> future = new CompletableFuture<T>();

    try {
      while (!semaphore.tryAcquire(50, TimeUnit.MILLISECONDS)) {
        if (closed.get()) {
          throw new IllegalStateException("LoaderExecutor is closed");
        }
      }
    } catch (InterruptedException e1) {
      throw new RuntimeException(e1);
    }

    try {
      commiting.increment();
      executor.execute(new QueueReleaseRunnable(new LoaderCommitObserver(alias, loader, future)));
    } catch (RejectedExecutionException rje) {
      semaphore.release();
      commiting.decrement();
      throw rje;
    }

    return future;
  }

  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      // wait for queue to empty and prevent anything else from being enqueued
      semaphore.acquireUninterruptibly(semaphoreSize);

      // wait for all asynchronously committing transactions to complete
      commiting.waitUntilZero();

      if (executor != null) {
        executor.shutdown();
        while (!executor.isTerminated()) {
          try {
            executor.awaitTermination(3, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }

      if (exceptionRef.get() != null) {
        throw new RuntimeException(exceptionRef.get());
      }

      // wait for any async mutations that transactions write to flush
      env.getSharedResources().getBatchWriter().waitForAsyncFlush();
    }
  }
}
