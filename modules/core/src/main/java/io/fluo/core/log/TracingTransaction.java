/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.core.log;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import io.fluo.api.client.Snapshot;
import io.fluo.api.client.Transaction;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.api.exceptions.AlreadySetException;
import io.fluo.api.exceptions.CommitException;
import io.fluo.api.iterator.RowIterator;
import io.fluo.core.impl.Notification;
import io.fluo.core.impl.TransactionImpl;
import io.fluo.core.impl.TxStats;
import io.fluo.core.impl.TxStringUtil;
import io.fluo.core.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingTransaction implements Transaction, Snapshot {

  private static final Logger log = LoggerFactory.getLogger(FluoConfiguration.TRANSACTION_PREFIX);
  private static final Logger collisionLog = LoggerFactory
      .getLogger(FluoConfiguration.TRANSACTION_PREFIX + ".collisions");
  private static final Logger summaryLog = LoggerFactory
      .getLogger(FluoConfiguration.TRANSACTION_PREFIX + ".summary");

  private final TransactionImpl tx;
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

  public TracingTransaction(TransactionImpl tx) {
    this(tx, null, null);
  }

  public TracingTransaction(TransactionImpl tx, Class<?> clazz) {
    this(tx, null, clazz);
  }

  private String encB(Collection<Bytes> columns) {
    return Iterators.toString(Iterators.transform(columns.iterator(),
        new Function<Bytes, String>() {
          @Override
          public String apply(Bytes b) {
            return Hex.encNonAscii(b);
          }
        }));
  }

  private String encRC(Collection<RowColumn> ret) {
    return Iterators.toString(Iterators.transform(ret.iterator(),
        new Function<RowColumn, String>() {
          @Override
          public String apply(RowColumn rc) {
            return Hex.encNonAscii(rc);
          }
        }));
  }

  private String encRC(Map<Bytes, Map<Column, Bytes>> ret) {
    return Iterators.toString(Iterators.transform(ret.entrySet().iterator(),
        new Function<Entry<Bytes, Map<Column, Bytes>>, String>() {
          @Override
          public String apply(Entry<Bytes, Map<Column, Bytes>> e) {
            return enc(e.getKey()) + "=" + encC(e.getValue());
          }
        }));
  }

  private String encC(Collection<Column> columns) {
    return Iterators.toString(Iterators.transform(columns.iterator(),
        new Function<Column, String>() {
          @Override
          public String apply(Column col) {
            return Hex.encNonAscii(col);
          }
        }));
  }

  private String encC(Map<Column, Bytes> ret) {
    return Iterators.toString(Iterators.transform(ret.entrySet().iterator(),
        new Function<Entry<Column, Bytes>, String>() {
          @Override
          public String apply(Entry<Column, Bytes> e) {
            return enc(e.getKey()) + "=" + enc(e.getValue());
          }
        }));
  }

  public TracingTransaction(TransactionImpl tx, Notification notification, Class<?> clazz) {
    this.tx = tx;
    this.txid = tx.getStartTs();

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
  public Map<Bytes, Map<Column, Bytes>> get(Collection<RowColumn> rowColumns) {
    Map<Bytes, Map<Column, Bytes>> ret = tx.get(rowColumns);
    if (log.isTraceEnabled()) {
      log.trace("txid: {} get({}) -> {}", txid, encRC(rowColumns), encRC(ret));
    }
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
    if (log.isTraceEnabled()) {
      log.trace("txid: {} setWeakNotification({}, {})", txid, enc(row), enc(col));
    }
    tx.setWeakNotification(row, col);
  }

  @Override
  public void setWeakNotification(String row, Column col) {
    setWeakNotification(Bytes.of(row), col);
  }

  @Override
  public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
    if (log.isTraceEnabled()) {
      log.trace("txid: {} set({}, {}, {})", txid, enc(row), enc(col), enc(value));
    }
    tx.set(row, col, value);
  }

  @Override
  public void set(String row, Column col, String value) throws AlreadySetException {
    set(Bytes.of(row), col, Bytes.of(value));
  }

  @Override
  public void delete(Bytes row, Column col) throws AlreadySetException {
    if (log.isTraceEnabled()) {
      log.trace("txid: {} delete({}, {})", txid, enc(row), enc(col));
    }
    tx.delete(row, col);
  }

  @Override
  public void delete(String row, Column col) {
    delete(Bytes.of(row), col);
  }

  @Override
  public void commit() throws CommitException {
    try {
      tx.commit();
      committed = true;
      log.trace("txid: {} commit() -> SUCCESSFUL commitTs: {}", txid, tx.getStats().getCommitTs());
    } catch (CommitException ce) {
      log.trace("txid: {} commit() -> UNSUCCESSFUL commitTs: {}", txid, tx.getStats().getCommitTs());

      if (!log.isTraceEnabled() && notification != null) {
        collisionLog.trace("txid: {} trigger: {} {} {}", txid, notification.getRow(),
            notification.getColumn(), notification.getTimestamp());
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
      if (clazz != null) {
        className = clazz.getSimpleName();
      }
      // TODO log total # read, see fluo-426
      summaryLog.trace(
          "txid: {} thread : {} time: {} ({} {} {}) #ret: {} #set: {} #collisions: {} "
              + "waitTime: {} committed: {} class: {}", txid, Thread.currentThread().getId(),
          stats.getTime(), stats.getPrecommitTime(), stats.getCommitPrimaryTime(),
          stats.getFinishCommitTime(), stats.getEntriesReturned(), stats.getEntriesSet(),
          stats.getCollisions(), stats.getLockWaitTime(), committed, className);
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

  @Override
  public String gets(String row, Column column) {
    return TxStringUtil.gets(this, row, column);
  }

  @Override
  public Map<Column, String> gets(String row, Set<Column> columns) {
    return TxStringUtil.gets(this, row, columns);
  }

  @Override
  public Map<String, Map<Column, String>> gets(Collection<String> rows, Set<Column> columns) {
    return TxStringUtil.gets(this, rows, columns);
  }

  @Override
  public Map<String, Map<Column, String>> gets(Collection<RowColumn> rowColumns) {
    return TxStringUtil.gets(this, rowColumns);
  }
}
