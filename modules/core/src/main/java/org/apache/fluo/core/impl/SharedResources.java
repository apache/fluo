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

package org.apache.fluo.core.impl;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.core.async.AsyncConditionalWriter;
import org.apache.fluo.core.async.CommitManager;
import org.apache.fluo.core.impl.TransactorCache.TcStatus;
import org.apache.fluo.core.impl.TransactorNode.TrStatus;
import org.apache.fluo.core.oracle.OracleClient;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.fluo.core.util.FluoExecutors;

/**
 * Shared Fluo resources that must be closed
 */
public class SharedResources implements AutoCloseable {

  private final Environment env;
  private final BatchWriter bw;
  private final ConditionalWriter cw;
  private final ConditionalWriter bulkCw;
  private final SharedBatchWriter sbw;
  private final CuratorFramework curator;
  private OracleClient oracleClient = null;
  private TransactorID tid = null;
  private TransactorNode tnode = null;
  private TransactorCache transactorCache = null;
  private TimestampTracker tsTracker = null;
  private volatile boolean isClosed = false;
  private final TxInfoCache txInfoCache;
  private final VisibilityCache visCache;
  private final MetricRegistry metricRegistry;

  private AsyncConditionalWriter acw;
  private AsyncConditionalWriter bulkAcw;
  private ExecutorService asyncCommitExecutor;
  private ExecutorService syncCommitExecutor;
  private CommitManager commitManager;


  public SharedResources(Environment env) throws TableNotFoundException {
    this.env = env;
    curator = CuratorUtil.newAppCurator(env.getConfiguration());
    curator.start();

    int numTservers = env.getConnector().instanceOperations().getTabletServers().size();
    int numBWThreads = FluoConfigurationImpl.getNumBWThreads(env.getConfiguration(), numTservers);
    bw = env.getConnector().createBatchWriter(env.getTable(),
        new BatchWriterConfig().setMaxWriteThreads(numBWThreads));
    sbw = new SharedBatchWriter(bw);

    int numCWThreads = FluoConfigurationImpl.getNumCWThreads(env.getConfiguration(), numTservers);
    cw = env.getConnector().createConditionalWriter(env.getTable(), new ConditionalWriterConfig()
        .setAuthorizations(env.getAuthorizations()).setMaxWriteThreads(numCWThreads));
    bulkCw =
        env.getConnector().createConditionalWriter(env.getTable(), new ConditionalWriterConfig()
            .setAuthorizations(env.getAuthorizations()).setMaxWriteThreads(numCWThreads));

    txInfoCache = new TxInfoCache(env);
    visCache = new VisibilityCache();
    metricRegistry = new MetricRegistry();

    int commitThreads = env.getConfiguration().getInt(FluoConfigurationImpl.ASYNC_COMMIT_THREADS,
        FluoConfigurationImpl.ASYNC_COMMIT_THREADS_DEFAULT);
    asyncCommitExecutor = FluoExecutors.newFixedThreadPool(commitThreads, "async-commits");

    commitThreads = env.getConfiguration().getInt(FluoConfigurationImpl.SYNC_COMMIT_THREADS,
        FluoConfigurationImpl.SYNC_COMMIT_THREADS_DEFAULT);
    syncCommitExecutor = FluoExecutors.newFixedThreadPool(commitThreads, "sync-commits");

    acw = new AsyncConditionalWriter(env, cw);
    bulkAcw = new AsyncConditionalWriter(env, bulkCw);
  }

  public SharedBatchWriter getBatchWriter() {
    checkIfClosed();
    return sbw;
  }

  public ConditionalWriter getConditionalWriter() {
    checkIfClosed();
    return cw;
  }

  public TxInfoCache getTxInfoCache() {
    checkIfClosed();
    return txInfoCache;
  }

  public CuratorFramework getCurator() {
    checkIfClosed();
    return curator;
  }

  public synchronized OracleClient getOracleClient() {
    checkIfClosed();
    if (oracleClient == null) {
      oracleClient = new OracleClient(env);
    }
    return oracleClient;
  }

  public synchronized TransactorID getTransactorID() {
    checkIfClosed();
    if (tid == null) {
      tid = new TransactorID(env);
    }
    return tid;
  }

  public synchronized TimestampTracker getTimestampTracker() {
    checkIfClosed();
    if (tsTracker == null) {
      tsTracker = new TimestampTracker(env, getTransactorID());
    }
    return tsTracker;
  }

  public synchronized TransactorNode getTransactorNode() {
    checkIfClosed();
    if (tnode == null) {
      tnode = new TransactorNode(env, getTransactorID());
    } else if (tnode.getStatus() == TrStatus.CLOSED) {
      throw new IllegalStateException("TransactorNode is closed!");
    }
    return tnode;
  }

  public synchronized TransactorCache getTransactorCache() {
    checkIfClosed();
    if (transactorCache == null) {
      transactorCache = new TransactorCache(env);
    } else if (transactorCache.getStatus() == TcStatus.CLOSED) {
      throw new IllegalStateException("TransactorCache is closed!");
    }
    return transactorCache;
  }

  public VisibilityCache getVisCache() {
    checkIfClosed();
    return visCache;
  }

  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  @Override
  public synchronized void close() {
    isClosed = true;

    if (commitManager != null) {
      commitManager.close();
    }
    asyncCommitExecutor.shutdownNow();
    try {
      asyncCommitExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e1) {
      throw new RuntimeException(e1);
    }

    syncCommitExecutor.shutdownNow();
    try {
      syncCommitExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e1) {
      throw new RuntimeException(e1);
    }

    if (tnode != null) {
      tnode.close();
    }
    if (tsTracker != null) {
      tsTracker.close();
    }
    if (transactorCache != null) {
      transactorCache.close();
    }
    if (oracleClient != null) {
      oracleClient.close();
    }
    acw.close();
    cw.close();
    bulkAcw.close();
    bulkCw.close();
    sbw.close();
    try {
      bw.close();
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
    curator.close();
  }

  private void checkIfClosed() {
    if (isClosed) {
      throw new IllegalStateException("SharedResources is closed!");
    }
  }

  public Executor getSyncCommitExecutor() {
    return syncCommitExecutor;
  }

  public Executor getAsyncCommitExecutor() {
    return asyncCommitExecutor;
  }

  public AsyncConditionalWriter getAsyncConditionalWriter() {
    return acw;
  }

  public AsyncConditionalWriter getBulkAsyncConditionalWriter() {
    return bulkAcw;
  }

  public synchronized CommitManager getCommitManager() {
    checkIfClosed();
    if (commitManager == null) {
      commitManager = new CommitManager(env);
    }
    return commitManager;
  }
}
