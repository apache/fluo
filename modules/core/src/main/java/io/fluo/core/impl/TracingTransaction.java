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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.exceptions.AlreadySetException;
import io.fluo.api.iterator.RowIterator;
import org.apache.log4j.Logger;

public class TracingTransaction implements TransactionBase {

  private TransactionBase tx;
  private long txid;

  private static final Logger log = Logger.getLogger(TracingTransaction.class);

  private static AtomicLong nextTxid = new AtomicLong(0);

  private void log(String format, Object... args) {
    String prefix = "txid:" + txid + " ";
    log.trace(String.format(prefix + format, args));
  }

  public TracingTransaction(TransactionBase tx) {
    this.tx = tx;
    this.txid = nextTxid.getAndIncrement();
  }

  @Override
  public Bytes get(Bytes row, Column column) throws Exception {
    Bytes ret = tx.get(row, column);
    log("get(%s, %s) -> %s", row, column, ret);
    return ret;
  }

  @Override
  public Map<Column,Bytes> get(Bytes row, Set<Column> columns) throws Exception {
    Map<Column,Bytes> ret = tx.get(row, columns);
    log("get(%s, %s) -> %s", row, columns, ret);
    return ret;
  }

  @Override
  public Map<Bytes,Map<Column,Bytes>> get(Collection<Bytes> rows, Set<Column> columns) throws Exception {
    Map<Bytes,Map<Column,Bytes>> ret = tx.get(rows, columns);
    // TODO make multiple log calls
    log("get(%s, %s) -> %s", rows, columns, ret);
    return ret;
  }

  @Override
  public RowIterator get(ScannerConfiguration config) throws Exception {
    // TODO log something better
    log("get(ScannerConfiguration");
    return tx.get(config);
  }

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    log("setWeakNotification(%s, %s)", row, col);
    tx.setWeakNotification(row, col);
  }

  @Override
  public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
    log("set(%s, %s, %s)", row, col, value);
    tx.set(row, col, value);
  }

  @Override
  public void delete(Bytes row, Column col) throws AlreadySetException {
    log("delete(%s, %s)", row, col);
    tx.delete(row, col);
  }

  static boolean isTracingEnabled() {
    return log.isTraceEnabled();
  }
}
