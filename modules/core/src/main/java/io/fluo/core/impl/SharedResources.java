/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.core.impl;

import java.io.Closeable;
import java.io.IOException;

import io.fluo.core.impl.TransactorCache.TcStatus;
import io.fluo.core.impl.TransactorID.TrStatus;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriterConfig;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/** 
 * Shared Fluo resources that must be closed
 */
public class SharedResources implements Closeable {

  private Environment env;
  private BatchWriter bw;
  private ConditionalWriter cw;
  private SharedBatchWriter sbw;
  private CuratorFramework curator;
  private TransactorID tid = null;
  private TransactorCache transactorCache = null;
  private volatile boolean isClosed = false;
  private TxInfoCache txInfoCache;

  public SharedResources(Environment env) throws TableNotFoundException {
    this.env = env;
    curator = CuratorFrameworkFactory.newClient(env.getConnector().getInstance().getZooKeepers(), 
        new ExponentialBackoffRetry(1000, 10));
    curator.start();
    bw = env.getConnector().createBatchWriter(env.getTable(), new BatchWriterConfig());
    sbw = new SharedBatchWriter(bw);
    cw = env.getConnector().createConditionalWriter(env.getTable(), 
        new ConditionalWriterConfig().setAuthorizations(env.getAuthorizations()));
    txInfoCache = new TxInfoCache(env);
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

  public synchronized TransactorID getTransactorID() {
    checkIfClosed();
    if (tid == null) {
      tid = new TransactorID(env);
    } else if (tid.getStatus() == TrStatus.CLOSED) {
      throw new IllegalStateException("Transactor is closed!");
    }
    return tid;
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
  
  @Override
  public synchronized void close() {
    isClosed = true;
    if (tid != null) {
      try {
        tid.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (transactorCache != null) {
      try {
        transactorCache.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    cw.close();
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
}
