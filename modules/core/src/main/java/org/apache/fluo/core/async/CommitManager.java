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

package org.apache.fluo.core.async;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Gauge;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.util.Limit;
import org.apache.fluo.core.worker.TxResult;
import org.slf4j.LoggerFactory;

/**
 * This class manage asynchronous commits of transactions. It blocks when transactions currently
 * asynchronously committing are using too much memory.
 *
 * <p>
 * This class also close transactions when finished and manages commit statistics so that each user
 * of the queue does not have to.
 */

public final class CommitManager {

  private Limit memoryLimit;
  private AtomicInteger commitingTransactions;

  public CommitManager(final Environment env) {
    memoryLimit = new Limit(FluoConfigurationImpl.getTxCommitMemory(env.getConfiguration()));
    commitingTransactions = new AtomicInteger(0);

    env.getSharedResources().getMetricRegistry().register(
        env.getMetricNames().getCommitsProcessing(),
        (Gauge<Integer>) () -> commitingTransactions.get());
  }


  private class CQCommitObserver implements AsyncCommitObserver {

    private final AsyncTransaction tx;
    private final AsyncCommitObserver aco;
    private final int size;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final String alias;

    private void finish(TxResult status) {
      if (finished.compareAndSet(false, true)) {
        commitingTransactions.decrementAndGet();
        tx.getStats().setCommitFinishTime(System.currentTimeMillis());
        tx.getStats().report(status.toString(), alias);
        memoryLimit.release(size);
        try {
          tx.close();
        } catch (Exception e) {
          LoggerFactory.getLogger(CommitManager.class).warn("Failed to close transaction ", e);
        }
      }
    }

    public CQCommitObserver(AsyncTransaction tx, AsyncCommitObserver aco, String alias, int size) {
      this.tx = tx;
      this.aco = aco;
      this.size = size;
      this.alias = alias;
    }

    @Override
    public void committed() {
      try {
        // perform cleanup and close tx before notifying observer
        finish(TxResult.COMMITTED);
      } finally {
        aco.committed();
      }
    }

    @Override
    public void failed(Throwable t) {
      try {
        // perform cleanup and close tx before notifying observer
        finish(TxResult.ERROR);
      } finally {
        aco.failed(t);
      }
    }

    @Override
    public void alreadyAcknowledged() {
      try {
        // perform cleanup and close tx before notifying observer
        finish(TxResult.AACKED);
      } finally {
        aco.alreadyAcknowledged();
      }
    }

    @Override
    public void commitFailed(String msg) {
      try {
        // perform cleanup and close tx before notifying observer
        finish(TxResult.COMMIT_EXCEPTION);
      } finally {
        aco.commitFailed(msg);
      }
    }
  }


  public void beginCommit(AsyncTransaction tx, String alias, AsyncCommitObserver aco) {
    Objects.requireNonNull(tx);
    Objects.requireNonNull(alias);
    Objects.requireNonNull(aco);

    int size = tx.getSize();
    memoryLimit.acquire(size);
    commitingTransactions.incrementAndGet();
    CQCommitObserver myAco = new CQCommitObserver(tx, aco, alias, size);
    tx.getStats().setCommitBeginTime(System.currentTimeMillis());
    tx.commitAsync(myAco);
  }

  /**
   * Waits for all transactions submitted before method called to complete.
   *
   */
  public void close() {
    // TODO should this wait?
  }
}
