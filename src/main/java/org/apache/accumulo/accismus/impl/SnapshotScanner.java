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
package org.apache.accumulo.accismus.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.accismus.Column;
import org.apache.accumulo.accismus.ColumnSet;
import org.apache.accumulo.accismus.ScannerConfiguration;
import org.apache.accumulo.accismus.StaleScanException;
import org.apache.accumulo.accismus.iterators.PrewriteIterator;
import org.apache.accumulo.accismus.iterators.RollbackCheckIterator;
import org.apache.accumulo.accismus.iterators.SnapshotIterator;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.UtilWaitThread;

import core.client.ConditionalWriter;
import core.client.ConditionalWriter.Status;
import core.client.impl.ConditionalWriterImpl;
import core.data.Condition;
import core.data.ConditionalMutation;

/**
 * 
 */
public class SnapshotScanner implements Iterator<Entry<Key,Value>> {
  
  private static final byte[] EMPTY = new byte[0];

  private long startTs;
  private Iterator<Entry<Key,Value>> iterator;
  private ScannerConfiguration config;
  private Connector conn;
  private String table;

  private static final long INITIAL_WAIT_TIME = 50;
  // TODO make configurable
  private static final long ROLLBACK_TIME = 5000;
  private static final long MAX_WAIT_TIME = 60000;

  public SnapshotScanner(Connector conn, String table, ScannerConfiguration config, long startTs) {
    this.table = table;
    this.conn = conn;
    this.config = config;
    this.startTs = startTs;
    
    setUpIterator();
  }
  
  private void setUpIterator() {
    // TODO make auths configurable
    Scanner scanner;
    try {
      scanner = conn.createScanner(table, new Authorizations());
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    config.configure(scanner);
    
    IteratorSetting iterConf = new IteratorSetting(10, SnapshotIterator.class);
    SnapshotIterator.setSnaptime(iterConf, startTs);
    scanner.addScanIterator(iterConf);
    
    this.iterator = scanner.iterator();
  }
  
  public boolean hasNext() {
    return iterator.hasNext();
  }
  
  public Entry<Key,Value> next() {
    
    long waitTime = INITIAL_WAIT_TIME;
    long firstSeen = -1;

    mloop: while (true) {
      Entry<Key,Value> entry = iterator.next();

      byte[] cf = entry.getKey().getColumnFamilyData().toArray();
      byte[] cq = entry.getKey().getColumnQualifierData().toArray();
      long colType = entry.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;

      if (colType == ColumnUtil.LOCK_PREFIX) {
        // TODO do read ahead while waiting for the lock
        
        boolean resolvedLock = false;

        if (firstSeen == -1) {
          firstSeen = System.currentTimeMillis();
          
          // the first time a lock is seen, try to resolve in case the transaction is complete, but this column is still locked.
          resolvedLock = resolveLock(entry, false);
        }

        if (!resolvedLock) {
          UtilWaitThread.sleep(waitTime);
          waitTime = Math.min(MAX_WAIT_TIME, waitTime * 2);
        
          if (System.currentTimeMillis() - firstSeen > ROLLBACK_TIME) {
            // try to abort the transaction
            resolveLock(entry, true);
          }
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
    List<ByteSequence> primary = ByteUtil.split(new ArrayByteSequence(entry.getValue().get()));
    
    ByteSequence prow = primary.get(0);
    ByteSequence pfam = primary.get(1);
    ByteSequence pqual = primary.get(2);
    ByteSequence pvis = primary.get(3);

    boolean isPrimary = entry.getKey().getRowData().equals(prow) && entry.getKey().getColumnFamilyData().equals(pfam)
        && entry.getKey().getColumnQualifierData().equals(pqual) && entry.getKey().getColumnVisibilityData().equals(pvis);

    long lockTs = entry.getKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
    
    boolean resolvedLock = false;

    if (isPrimary) {
      if (abort) {
        rollbackPrimary(prow, pfam, pqual, pvis, lockTs, entry.getValue().get());
        resolvedLock = true;
      }
    } else {

      // TODO make auths configurable
      // TODO ensure primary is visible
      // TODO reususe scanner?
      try {
        Scanner scanner = conn.createScanner(table, new Authorizations());
        IteratorSetting is = new IteratorSetting(10, RollbackCheckIterator.class);
        RollbackCheckIterator.setLocktime(is, lockTs);
        scanner.addScanIterator(is);
        scanner.setRange(Range.exact(ByteUtil.toText(prow), ByteUtil.toText(pfam), ByteUtil.toText(pqual), ByteUtil.toText(pvis)));
        
        Iterator<Entry<Key,Value>> iter = scanner.iterator();
        
        if (iter.hasNext()) {
          // TODO verify what comes back from iterator
          Entry<Key,Value> entry2 = iter.next();
          long colType = entry2.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;
          if (colType == ColumnUtil.DEL_LOCK_PREFIX) {
            if(new DelLockValue(entry2.getValue().get()).isRollback()){ 
              rollback(entry.getKey(), lockTs);
              resolvedLock = true;
            } else {
              long commitTs = entry2.getKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
              commitColumn(entry, lockTs, commitTs);
              resolvedLock = true;
            }
          } else if (colType == ColumnUtil.WRITE_PREFIX) {
            // TODO ensure value == lockTs
            long commitTs = entry2.getKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
            commitColumn(entry, lockTs, commitTs);
            resolvedLock = true;
          } else if (colType == ColumnUtil.LOCK_PREFIX) {
            if (abort) {
              if (rollbackPrimary(prow, pfam, pqual, pvis, lockTs, entry.getValue().get())) {
                rollback(entry.getKey(), lockTs);
                resolvedLock = true;
              }
            }
          } else {
            // TODO
            throw new IllegalStateException();
          }
        } else {
          // TODO no info about the status of this tx
          throw new IllegalStateException();
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
    boolean isTrigger = lv.getObserver().length() > 0;
    // TODO cache col vis
    Column col = new Column(entry.getKey().getColumnFamilyData(), entry.getKey().getColumnQualifierData()).setVisibility(entry.getKey()
        .getColumnVisibilityParsed());
    Mutation m = new Mutation(entry.getKey().getRowData().toArray());
    
    // TODO pass observed cols
    ColumnSet observedColumns = new ColumnSet();
    ColumnUtil.commitColumn(isTrigger, false, col, lv.isWrite(), lockTs, commitTs, observedColumns, m);
    
    try {
      // TODO use conditional writer?
      BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
      bw.addMutation(m);
      bw.close();
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  private void rollback(Key k, long lockTs) {
    Mutation mut = new Mutation(k.getRowData().toArray());
    mut.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(), k.getColumnVisibilityParsed(), ColumnUtil.DEL_LOCK_PREFIX | startTs,
        DelLockValue.encode(lockTs, false, true));
    
    try {
      // TODO use conditional writer?
      BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
      bw.addMutation(mut);
      bw.close();
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
  }

  boolean rollbackPrimary(ByteSequence prow, ByteSequence pfam, ByteSequence pqual, ByteSequence pvis, long lockTs, byte[] val) {
    // TODO use cached CV
    ColumnVisibility cv = new ColumnVisibility(pvis.toArray());
    
    // TODO avoid conversions to arrays
    // TODO review use of PrewriteIter here
    ConditionalMutation delLockMutation = new ConditionalMutation(prow.toArray());
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    // TODO cache col vis?
    delLockMutation.addCondition(new Condition(pfam, pqual).setIterators(iterConf).setVisibility(cv).setValue(val));
    
    // TODO maybe do scan 1st and conditional write 2nd.... for a tx w/ many columns, the conditional write will only succeed once... could check if its the
    // primary or not
    // TODO sanity check on lockTs vs startTs
    
    delLockMutation.put(pfam.toArray(), pqual.toArray(), cv, ColumnUtil.DEL_LOCK_PREFIX | startTs, DelLockValue.encode(lockTs, true, true));
    
    // TODO make auths configurable
    ConditionalWriter cw = new ConditionalWriterImpl(table, conn, new Authorizations());
    
    // TODO handle other conditional writer cases
    return cw.write(delLockMutation).getStatus() == Status.ACCEPTED;
  }

  public void remove() {
    iterator.remove();
  }
  
}
