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
package io.fluo.core.log;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import io.fluo.api.client.Snapshot;
import io.fluo.api.client.Transaction;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.exceptions.AlreadySetException;
import io.fluo.api.exceptions.CommitException;
import io.fluo.api.iterator.RowIterator;
import io.fluo.core.impl.TransactionImpl;
import io.fluo.core.impl.TxStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingTransaction implements Transaction, Snapshot {

  private static final Logger log = LoggerFactory.getLogger(FluoConfiguration.TRANSACTION_PREFIX);
  private static final Logger collisionLog = LoggerFactory.getLogger(FluoConfiguration.TRANSACTION_PREFIX+".collisions");
  private static final Logger summaryLog = LoggerFactory.getLogger(FluoConfiguration.TRANSACTION_PREFIX+".summary");
  
  private final TransactionImpl tx;
  private final long txid;
  private Bytes triggerRow;
  private Column triggerColumn;
  private Class<?> clazz;
  private boolean committed = false;
  
  public TracingTransaction(TransactionImpl tx){
    this(tx, null, null, null);
  }
  
  public TracingTransaction(TransactionImpl tx, Class<?> clazz){
    this(tx, null, null, clazz);
  }
  
  public TracingTransaction(TransactionImpl tx, Bytes triggerRow, Column triggerColumn, Class<?> clazz) {
    this.tx = tx;
    this.txid = tx.getStartTs();
    
    this.triggerRow = triggerRow;
    this.triggerColumn = triggerColumn;
    this.clazz = clazz;
    
    log.trace("txid: {} begin() thread: {}", txid, Thread.currentThread().getId());

    if(triggerRow != null){
      log.trace("txid: {} trigger: {} {}", txid, triggerRow, triggerColumn);
    }
    
    if(clazz != null){
      log.trace("txid: {} class: {}", txid, clazz.getName());
    }
    
  }

  @Override
  public Bytes get(Bytes row, Column column) {
    Bytes ret = tx.get(row, column);
    log.trace("txid: {} get({}, {}) -> {}", txid, row, column, ret);
    return ret;
  }

  @Override
  public Map<Column,Bytes> get(Bytes row, Set<Column> columns) {
    Map<Column,Bytes> ret = tx.get(row, columns);
    log.trace("txid: {} get({}, {}) -> {}", txid, row, columns, ret);
    return ret;
  }

  @Override
  public Map<Bytes,Map<Column,Bytes>> get(Collection<Bytes> rows, Set<Column> columns) {
    Map<Bytes,Map<Column,Bytes>> ret = tx.get(rows, columns);
    log.trace("txid: {} get({}, {}) -> {}", txid, rows, columns, ret);
    return ret;
  }

  @Override
  public RowIterator get(ScannerConfiguration config) {
    // TODO log something better (see fluo-425)
    log.trace("txid: {} get(ScannerConfiguration)", txid);
    return tx.get(config);
  }

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    log.trace("txid: {} setWeakNotification({}, {})", txid, row, col);
    tx.setWeakNotification(row, col);
  }

  @Override
  public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
    log.trace("txid: {} set({}, {}, {})", txid, row, col, value);
    tx.set(row, col, value);
  }

  @Override
  public void delete(Bytes row, Column col) throws AlreadySetException {
    log.trace("txid: {} delete({}, {})", txid, row, col);
    tx.delete(row, col);
  }

  @Override
  public void commit() throws CommitException {
    try {
      tx.commit();
      committed = true;
      log.trace("txid: {} commit() -> SUCCESSFUL commitTs: {}", txid, tx.getStats().getCommitTs());
    } catch (CommitException ce) {
      log.trace("txid: {} commit() -> UNSUCCESSFUL commitTs: {}", txid, tx.getStats().getCommitTs());

      if (!log.isTraceEnabled() && triggerRow != null) {
        collisionLog.trace("txid: {} trigger: {} {}", txid, triggerRow, triggerColumn);
      }

      if (!log.isTraceEnabled() && clazz != null) {
        collisionLog.trace("txid: {} class: {}", txid, clazz.getName());
      }

      collisionLog.trace("txid: {} collisions: {}", txid, tx.getStats().getRejected());

      throw ce;
    }
  }

  @Override
  public void close() {
    log.trace("txid: {} close()", txid);
    if (summaryLog.isTraceEnabled()) {
      TxStats stats = tx.getStats();
      String className = "N/A";
      if (clazz != null)
        className = clazz.getSimpleName();
      //TODO log total # read, see fluo-426
      summaryLog.trace("txid: {} thread : {} time: {} #ret: {} #set: {} #collisions: {} waitTime: {} %s committed: {} class: {}", txid, Thread
          .currentThread().getId(), stats.getTime(), stats.getEntriesReturned(), stats.getEntriesSet(), stats.getCollisions(), stats.getLockWaitTime(),
          committed, className);
    }
    tx.close();
  }

  public static boolean isTracingEnabled() {
    return log.isTraceEnabled() || summaryLog.isTraceEnabled() || collisionLog.isTraceEnabled();
  }
}