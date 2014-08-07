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

import io.fluo.api.Column;
import io.fluo.api.ScannerConfiguration;
import io.fluo.api.exceptions.StaleScanException;
import io.fluo.core.util.UtilWaitThread;
import io.fluo.impl.iterators.SnapshotIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

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

  static final long INITIAL_WAIT_TIME = 50;
  // TODO make configurable
  static final long MAX_WAIT_TIME = 60000;

  public SnapshotScanner(Configuration aconfig, ScannerConfiguration config, long startTs, TxStats stats) {
    this.aconfig = aconfig;
    this.config = config;
    this.startTs = startTs;
    this.stats = stats;
    setUpIterator();
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
  
  private void resetScanner(Range range) {
    try {
      config = (ScannerConfiguration) config.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }

    config.setRange(range);
    setUpIterator();
  }

  public void resolveLock(Entry<Key,Value> lockEntry) {

    // read ahead a little bit looking for other locks to resolve

    long startTime = System.currentTimeMillis();
    long waitTime = INITIAL_WAIT_TIME;

    List<Entry<Key,Value>> locks = new ArrayList<>();
    locks.add(lockEntry);
    int amountRead = 0;
    int numRead = 0;

    Key origEndKey = config.getRange().getEndKey();
    boolean isEndInclusive = config.getRange().isEndKeyInclusive();

    while (true) {
      while (iterator.hasNext()) {
        Entry<Key,Value> entry = iterator.next();

        long colType = entry.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;

        if (colType == ColumnUtil.LOCK_PREFIX) {
          locks.add(entry);
        }

        amountRead += entry.getKey().getSize() + entry.getValue().getSize();
        numRead++;

        if (numRead > 100 || amountRead > 1 << 12) {
          break;
        }
      }

      boolean resolvedLocks = LockResolver.resolveLocks(aconfig, startTs, stats, locks, startTime);

      if (!resolvedLocks) {
        UtilWaitThread.sleep(waitTime);
        stats.incrementLockWaitTime(waitTime);
        waitTime = Math.min(MAX_WAIT_TIME, waitTime * 2);

        Key startKey = new Key(locks.get(0).getKey());
        startKey.setTimestamp(Long.MAX_VALUE);

        Key endKey = locks.get(locks.size() - 1).getKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS);

        resetScanner(new Range(startKey, true, endKey, false));

        locks.clear();

      } else {
        break;
      }
    }

    Key startKey = new Key(lockEntry.getKey());
    startKey.setTimestamp(Long.MAX_VALUE);

    resetScanner(new Range(startKey, true, origEndKey, isEndInclusive));
  }

  public Entry<Key,Value> getNext() {
    mloop: while (true) {
      // its possible a next could exist then be rolled back
      if (!iterator.hasNext())
        return null;

      Entry<Key,Value> entry = iterator.next();

      long colType = entry.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;

      if (colType == ColumnUtil.LOCK_PREFIX) {
        resolveLock(entry);
        continue mloop;
      } else if (colType == ColumnUtil.DATA_PREFIX) {
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

  public void remove() {
    iterator.remove();
  }
}
