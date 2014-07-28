/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.impl;

import io.fluo.api.Bytes;
import io.fluo.api.Column;
import io.fluo.api.ScannerConfiguration;
import io.fluo.api.exceptions.StaleScanException;
import io.fluo.core.util.UtilWaitThread;
import io.fluo.impl.iterators.PrewriteIterator;
import io.fluo.impl.iterators.SnapshotIterator;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.mutable.MutableLong;

/**
 * 
 */
public class SnapshotScanner implements Iterator<Entry<Key,Value>> {

  private long startTs;
  private Iterator<Entry<Key,Value>> iterator;
  private Entry<Key,Value> next;
  private ScannerConfiguration config;

  private Configuration aconfig;
  private TxStats stats;
  private TransactorCache cache;

  static final long INITIAL_WAIT_TIME = 50;
  // TODO make configurable
  static final long MAX_WAIT_TIME = 60000;

  public SnapshotScanner(Configuration aconfig, ScannerConfiguration config, long startTs, TxStats stats) {
    this.aconfig = aconfig;
    this.config = config;
    this.startTs = startTs;
    this.stats = stats;
    setUpIterator();
    this.cache = aconfig.getSharedResources().getTransactorCache();
  }
  
  private void setUpIterator() {
    Scanner scanner;
    try {
      scanner = aconfig.getConnector().createScanner(aconfig.getTable(), aconfig.getAuthorizations());
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    scanner.clearColumns();
    scanner.clearScanIterators();
    
    scanner.setRange(config.getRange());

    setupScanner(scanner, config.getColumns(), startTs);
    
    this.iterator = scanner.iterator();
  }

  static void setupScanner(ScannerBase scanner, List<Column> columns, long startTs) {
    for (Column col : columns) {
      if (col.getQualifier() != null) {
        scanner.fetchColumn(ByteUtil.toText(col.getFamily()), ByteUtil.toText(col.getQualifier()));
      } else {
        scanner.fetchColumnFamily(ByteUtil.toText(col.getFamily()));
      }
    }
    
    IteratorSetting iterConf = new IteratorSetting(10, SnapshotIterator.class);
    SnapshotIterator.setSnaptime(iterConf, startTs);
    scanner.addScanIterator(iterConf);
  }
  
  public boolean hasNext() {
    if (next == null) {
      next = getNext();
    }
    
    return next != null;
  }
  
  public Entry<Key,Value> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    
    Entry<Key,Value> tmp = next;
    next = null;
    return tmp;
  }
  
  public Entry<Key,Value> getNext() {
    
    long waitTime = INITIAL_WAIT_TIME;
    long firstSeen = -1;

    mloop: while (true) {
      // its possible a next could exist then be rolled back
      if (!iterator.hasNext())
        return null;

      Entry<Key,Value> entry = iterator.next();

      byte[] cf = entry.getKey().getColumnFamilyData().toArray();
      byte[] cq = entry.getKey().getColumnQualifierData().toArray();
      long colType = entry.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;

      if (colType == ColumnUtil.LOCK_PREFIX) {
        // TODO do read ahead while waiting for the lock... this is important for the case where reprocessing a failed transaction... need to find a batch or
        // locked columns and resolve them together, not one by one... should cache the status of the primary lock while doing this
        
        if (firstSeen == -1) {
          firstSeen = System.currentTimeMillis();
        }

        LockValue lv = new LockValue(entry.getValue().get());

        // abort the transaction if transactor has died or timeout occurred
        boolean resolvedLock = false;
        if (!cache.checkExists(lv.getTransactor())) { 
          stats.incrementDeadLocks();
          resolvedLock = resolveLock(entry, true);
        } else if ((System.currentTimeMillis() - firstSeen) > aconfig.getRollbackTime()) {
          stats.incrementTimedOutLocks();
          resolvedLock = resolveLock(entry, true);
        }
        
        if (!resolvedLock) {
          UtilWaitThread.sleep(waitTime);
          stats.incrementLockWaitTime(waitTime);
          waitTime = Math.min(MAX_WAIT_TIME, waitTime * 2);
        }

        Key k = entry.getKey();
        Key start = new Key(k.getRowData().toArray(), cf, cq, k.getColumnVisibilityData().toArray(), Long.MAX_VALUE);
        
        try {
          config = (ScannerConfiguration) config.clone();
        } catch (CloneNotSupportedException e) {
          throw new RuntimeException(e);
        }
        config.setRange(new Range(start, true, config.getRange().getEndKey(), config.getRange().isEndKeyInclusive()));
        setUpIterator();

        continue mloop;
      } else if (colType == ColumnUtil.DATA_PREFIX) {
        waitTime = INITIAL_WAIT_TIME;
        firstSeen = -1;
        stats.incrementEntriesReturned(1);
        return entry;
      } else if (colType == ColumnUtil.WRITE_PREFIX) {
        if (WriteValue.isTruncated(entry.getValue().get())) {
          throw new StaleScanException();
        } else {
          throw new IllegalArgumentException();
        }
      } else {
        throw new IllegalArgumentException();
      }
    }
  }
  
  private boolean resolveLock(Entry<Key,Value> entry, boolean abort) {

    // TODO need to check in zookeeper if worker processing tx is alive

    List<Bytes> primary = Bytes.split(Bytes.wrap(entry.getValue().get()));
    
    Bytes prow = primary.get(0);
    Bytes pfam = primary.get(1);
    Bytes pqual = primary.get(2);
    Bytes pvis = primary.get(3);
    
    Bytes erow = Bytes.wrap(entry.getKey().getRowData().toArray());
    Bytes efam = Bytes.wrap(entry.getKey().getColumnFamilyData().toArray());
    Bytes equal = Bytes.wrap(entry.getKey().getColumnQualifierData().toArray());
    Bytes evis = Bytes.wrap(entry.getKey().getColumnVisibilityData().toArray());

    boolean isPrimary = erow.equals(prow) && efam.equals(pfam) && equal.equals(pqual) && evis.equals(pvis);

    long lockTs = entry.getKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
    
    boolean resolvedLock = false;

    if (isPrimary) {
      if (abort) {
        try {
          rollbackPrimary(prow, pfam, pqual, pvis, lockTs, entry.getValue().get());
        } catch (AccumuloException e) {
          throw new RuntimeException(e);
        } catch (AccumuloSecurityException e) {
          throw new RuntimeException(e);
        }
        resolvedLock = true;
      }
    } else {

      // TODO ensure primary is visible
      // TODO reususe scanner?
      try {
        
        Value lockVal = new Value();
        MutableLong commitTs = new MutableLong(-1);
        // TODO use cached CV
        TxStatus txStatus = TxStatus.getTransactionStatus(aconfig, prow, new Column(pfam, pqual)
            .setVisibility(new ColumnVisibility(ByteUtil.toText(pvis))), lockTs, commitTs, lockVal);
        
        // TODO cache status

        switch (txStatus) {
          case COMMITTED:
            if (commitTs.longValue() < lockTs) {
              throw new IllegalStateException("bad commitTs : " + prow + " " + pfam + " " + pqual + " " + pvis + " (" + commitTs.longValue() + "<" + lockTs
                  + ")");
            }
            commitColumn(entry, lockTs, commitTs.longValue());
            resolvedLock = true;
            break;
          case LOCKED:
            if (abort) {
              if (rollbackPrimary(prow, pfam, pqual, pvis, lockTs, lockVal.get())) {
                rollback(entry.getKey(), lockTs);
                resolvedLock = true;
              }
            }
            break;
          case ROLLED_BACK:
            // TODO ensure this if ok if there concurrent rollback
            rollback(entry.getKey(), lockTs);
            resolvedLock = true;
            break;
          case UNKNOWN:
            if (abort) {
              throw new IllegalStateException("can not abort : " + prow + " " + pfam + " " + pqual + " " + pvis + " (" + txStatus + ")");
            }
            break;
        }

      } catch (Exception e) {
        // TODO proper exception handling
        throw new RuntimeException(e);
      }
    }
    
    return resolvedLock;
  }

  private void commitColumn(Entry<Key,Value> entry, long lockTs, long commitTs) {
    LockValue lv = new LockValue(entry.getValue().get());
    // TODO cache col vis
    Bytes cf = Bytes.wrap(entry.getKey().getColumnFamilyData().toArray());
    Bytes cq = Bytes.wrap(entry.getKey().getColumnQualifierData().toArray());
    Column col = new Column(cf, cq).setVisibility(entry.getKey().getColumnVisibilityParsed());
    Mutation m = new Mutation(entry.getKey().getRowData().toArray());
    
    ColumnUtil.commitColumn(lv.isTrigger(), false, col, lv.isWrite(), lv.isDelete(), lockTs, commitTs, aconfig.getObservers().keySet(), m);
    
    // TODO use conditional writer?
    aconfig.getSharedResources().getBatchWriter().writeMutation(m);
  }

  private void rollback(Key k, long lockTs) {
    Mutation mut = new Mutation(k.getRowData().toArray());
    mut.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(), k.getColumnVisibilityParsed(), ColumnUtil.DEL_LOCK_PREFIX | startTs,
        DelLockValue.encode(lockTs, false, true));
    
    // TODO use conditional writer?
    aconfig.getSharedResources().getBatchWriter().writeMutation(mut);
  }

  boolean rollbackPrimary(Bytes prow, Bytes pfam, Bytes pqual, Bytes pvis, long lockTs, byte[] val) throws AccumuloException,
      AccumuloSecurityException {
    // TODO use cached CV
    ColumnVisibility cv = new ColumnVisibility(ByteUtil.toText(pvis));
    
    // TODO avoid conversions to arrays
    // TODO review use of PrewriteIter here

    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    // TODO cache col vis?
    ConditionalMutation delLockMutation = new ConditionalMutation(ByteUtil.toByteSequence(prow), 
        new Condition(ByteUtil.toByteSequence(pfam), ByteUtil.toByteSequence(pqual)).setIterators(iterConf).setVisibility(cv).setValue(val));
    
    // TODO sanity check on lockTs vs startTs
    
    delLockMutation.put(ByteUtil.toText(pfam), ByteUtil.toText(pqual), cv, ColumnUtil.DEL_LOCK_PREFIX | startTs, 
          new Value(DelLockValue.encode(lockTs, true, true)));
    
    ConditionalWriter cw = null;
    
    cw = aconfig.getSharedResources().getConditionalWriter();

    // TODO handle other conditional writer cases
    return cw.write(delLockMutation).getStatus() == Status.ACCEPTED;
  }

  public void remove() {
    iterator.remove();
  }
}
