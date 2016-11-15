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

package org.apache.fluo.core.log;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Iterators;
import org.apache.fluo.api.client.AbstractTransactionBase;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.ScannerBuilder;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.exceptions.AlreadySetException;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.core.async.AsyncCommitObserver;
import org.apache.fluo.core.async.AsyncTransaction;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.impl.TxStats;
import org.apache.fluo.core.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingTransaction extends AbstractTransactionBase implements AsyncTransaction,
    Snapshot {

  private static final Logger log = LoggerFactory.getLogger(FluoConfiguration.TRANSACTION_PREFIX);
  private static final Logger collisionLog = LoggerFactory
      .getLogger(FluoConfiguration.TRANSACTION_PREFIX + ".collisions");
  private static final Logger summaryLog = LoggerFactory
      .getLogger(FluoConfiguration.TRANSACTION_PREFIX + ".summary");

  private final AsyncTransaction tx;
  private final long txid;
  private Notification notification;
  private Class<?> clazz;
  private boolean committed = false;

  private static String enc(Bytes b) {
    return Hex.encNonAscii(b);
  }

  private static String enc(Column c) {
    return Hex.encNonAscii(c);
  }

  public TracingTransaction(AsyncTransaction tx) {
    this(tx, null, null);
  }

  public TracingTransaction(AsyncTransaction tx, Class<?> clazz) {
    this(tx, null, clazz);
  }

  private String encB(Collection<Bytes> columns) {
    return Iterators.toString(Iterators.transform(columns.iterator(), Hex::encNonAscii));
  }

  private String encRC(Collection<RowColumn> ret) {
    return Iterators.toString(Iterators.transform(ret.iterator(), Hex::encNonAscii));
  }

  private String encRC(Map<Bytes, Map<Column, Bytes>> ret) {
    return Iterators.toString(Iterators.transform(ret.entrySet().iterator(), e -> enc(e.getKey())
        + "=" + encC(e.getValue())));
  }

  private String encRCM(Map<RowColumn, Bytes> ret) {
    return Iterators.toString(Iterators.transform(ret.entrySet().iterator(),
        e -> Hex.encNonAscii(e.getKey()) + "=" + enc(e.getValue())));
  }


  private String encC(Collection<Column> columns) {
    return Iterators.toString(Iterators.transform(columns.iterator(), Hex::encNonAscii));
  }

  private String encC(Map<Column, Bytes> ret) {
    return Iterators.toString(Iterators.transform(ret.entrySet().iterator(), e -> enc(e.getKey())
        + "=" + enc(e.getValue())));
  }

  public TracingTransaction(AsyncTransaction tx, Notification notification, Class<?> clazz) {
    this.tx = tx;
    this.txid = tx.getStartTimestamp();

    this.notification = notification;
    this.clazz = clazz;

    if (log.isTraceEnabled()) {
      log.trace("txid: {} begin() thread: {}", txid, Thread.currentThread().getId());

      if (notification != null) {
        log.trace("txid: {} trigger: {} {} {}", txid, enc(notification.getRow()),
            enc(notification.getColumn()), notification.getTimestamp());
      }

      if (clazz != null) {
        log.trace("txid: {} class: {}", txid, clazz.getName());
      }
    }

  }

  @Override
  public Bytes get(Bytes row, Column column) {
    Bytes ret = tx.get(row, column);
    if (log.isTraceEnabled()) {
      log.trace("txid: {} get({}, {}) -> {}", txid, enc(row), enc(column), enc(ret));
    }
    return ret;
  }

  @Override
  public Map<Column, Bytes> get(Bytes row, Set<Column> columns) {
    Map<Column, Bytes> ret = tx.get(row, columns);
    if (log.isTraceEnabled()) {
      log.trace("txid: {} get({}, {}) -> {}", txid, enc(row), encC(columns), encC(ret));
    }
    return ret;
  }

  @Override
  public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns) {
    Map<Bytes, Map<Column, Bytes>> ret = tx.get(rows, columns);
    if (log.isTraceEnabled()) {
      log.trace("txid: {} get({}, {}) -> {}", txid, encB(rows), encC(columns), encRC(ret));
    }
    return ret;
  }

  @Override
  public Map<RowColumn, Bytes> get(Collection<RowColumn> rowColumns) {
    Map<RowColumn, Bytes> ret = tx.get(rowColumns);
    if (log.isTraceEnabled()) {
      log.trace("txid: {} get({}) -> {}", txid, encRC(rowColumns), encRCM(ret));
    }
    return ret;
  }

  @Override
  public ScannerBuilder scanner() {
    return new TracingScannerBuilder(tx.scanner(), txid);
  }

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    if (log.isTraceEnabled()) {
      log.trace("txid: {} setWeakNotification({}, {})", txid, enc(row), enc(col));
    }
    tx.setWeakNotification(row, col);
  }

  @Override
  public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
    if (log.isTraceEnabled()) {
      log.trace("txid: {} set({}, {}, {})", txid, enc(row), enc(col), enc(value));
    }
    tx.set(row, col, value);
  }

  @Override
  public void delete(Bytes row, Column col) throws AlreadySetException {
    if (log.isTraceEnabled()) {
      log.trace("txid: {} delete({}, {})", txid, enc(row), enc(col));
    }
    tx.delete(row, col);
  }

  @Override
  public void commit() throws CommitException {
    try {
      tx.commit();
      committed = true;
      log.trace("txid: {} commit() -> SUCCESSFUL commitTs: {}", txid, tx.getStats().getCommitTs());
    } catch (CommitException ce) {
      logUnsuccessfulCommit();
      throw ce;
    }
  }

  private void logUnsuccessfulCommit() {
    log.trace("txid: {} commit() -> UNSUCCESSFUL commitTs: {}", txid, tx.getStats().getCommitTs());

    if (!log.isTraceEnabled() && notification != null) {
      collisionLog.trace("txid: {} trigger: {} {} {}", txid, notification.getRow(),
          notification.getColumn(), notification.getTimestamp());
    }

    if (!log.isTraceEnabled() && clazz != null) {
      collisionLog.trace("txid: {} class: {}", txid, clazz.getName());
    }

    collisionLog.trace("txid: {} collisions: {}", txid, tx.getStats().getRejected());
  }

  @Override
  public void close() {
    log.trace("txid: {} close()", txid);
    if (summaryLog.isTraceEnabled()) {
      TxStats stats = tx.getStats();
      String className = "N/A";
      if (clazz != null) {
        className = clazz.getSimpleName();
      }
      // TODO log total # read, see fluo-426
      summaryLog.trace("txid: {} thread : {} time: {} ({} {}) #ret: {} #set: {} #collisions: {} "
          + "waitTime: {} committed: {} class: {}", txid, Thread.currentThread().getId(),
          stats.getTime(), stats.getReadTime(), stats.getCommitTime(), stats.getEntriesReturned(),
          stats.getEntriesSet(), stats.getCollisions(), stats.getLockWaitTime(), committed,
          className);
    }
    tx.close();
  }

  public static boolean isTracingEnabled() {
    return log.isTraceEnabled() || summaryLog.isTraceEnabled() || collisionLog.isTraceEnabled();
  }

  @Override
  public long getStartTimestamp() {
    return tx.getStartTimestamp();
  }

  public class LoggingCommitObserver implements AsyncCommitObserver {

    AsyncCommitObserver aco;

    LoggingCommitObserver(AsyncCommitObserver aco) {
      this.aco = aco;
    }

    @Override
    public void committed() {
      log.trace("txid: {} commit() -> SUCCESSFUL commitTs: {}", txid, tx.getStats().getCommitTs());
      committed = true;
      aco.committed();
    }

    @Override
    public void failed(Throwable t) {
      aco.failed(t);
      log.trace("txid: {} failed {}", txid, t.getMessage());
    }

    @Override
    public void alreadyAcknowledged() {
      aco.alreadyAcknowledged();
      logUnsuccessfulCommit();
    }

    @Override
    public void commitFailed(String msg) {
      aco.commitFailed(msg);
      logUnsuccessfulCommit();
    }

  }

  @Override
  public void commitAsync(AsyncCommitObserver commitCallback) {
    tx.commitAsync(new LoggingCommitObserver(commitCallback));
  }

  @Override
  public TxStats getStats() {
    return tx.getStats();
  }

  @Override
  public int getSize() {
    return tx.getSize();
  }
}
