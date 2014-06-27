package org.fluo.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.log4j.Logger;

import org.fluo.api.Column;
import org.fluo.api.RowIterator;
import org.fluo.api.ScannerConfiguration;
import org.fluo.api.Transaction;

public class TracingTransaction implements Transaction {

  private Transaction tx;
  private long txid;

  private static final Logger log = Logger.getLogger(TracingTransaction.class);

  private static AtomicLong nextTxid = new AtomicLong(0);

  private void log(String format, Object... args) {
    String prefix = "txid:" + txid + " ";
    log.trace(String.format(prefix + format, args));
  }

  public TracingTransaction(Transaction tx) {
    this.tx = tx;
    this.txid = nextTxid.getAndIncrement();
  }

  @Override
  public ByteSequence get(ByteSequence row, Column column) throws Exception {
    ByteSequence ret = tx.get(row, column);
    log("get(%s, %s) -> %s", row, column, ret);
    return ret;
  }

  @Override
  public Map<Column,ByteSequence> get(ByteSequence row, Set<Column> columns) throws Exception {
    Map<Column,ByteSequence> ret = tx.get(row, columns);
    log("get(%s, %s) -> %s", row, columns, ret);
    return ret;
  }

  @Override
  public Map<ByteSequence,Map<Column,ByteSequence>> get(Collection<ByteSequence> rows, Set<Column> columns) throws Exception {
    Map<ByteSequence,Map<Column,ByteSequence>> ret = tx.get(rows, columns);
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
  public void setWeakNotification(ByteSequence row, Column col) {
    log("setWeakNotification(%s, %s)", row, col);
    tx.setWeakNotification(row, col);
  }

  @Override
  public void set(ByteSequence row, Column col, ByteSequence value) {
    log("set(%s, %s, %s)", row, col, value);
    tx.set(row, col, value);
  }

  @Override
  public void delete(ByteSequence row, Column col) {
    log("delete(%s, %s)", row, col);
    tx.delete(row, col);
  }

  static boolean isTracingEnabled() {
    return log.isTraceEnabled();
  }
}
