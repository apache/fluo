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
import java.util.Map.Entry;

import org.apache.accumulo.accismus.ScannerConfiguration;
import org.apache.accumulo.accismus.iterators.SnapshotIterator;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.UtilWaitThread;

/**
 * 
 */
public class SnapshotScanner implements Iterator<Entry<Key,Value>> {
  
  private Scanner scanner;
  private long startTs;
  private Iterator<Entry<Key,Value>> iterator;
  private ScannerConfiguration config;

  public SnapshotScanner(Scanner scanner, ScannerConfiguration config, long startTs) {
    this.scanner = scanner;
    this.config = config;
    this.startTs = startTs;
    
    setUpIterator();
  }
  
  private void setUpIterator() {
    config.configure(scanner);
    
    IteratorSetting iterConf = new IteratorSetting(1, SnapshotIterator.class);
    SnapshotIterator.setSnaptime(iterConf, startTs);
    scanner.addScanIterator(iterConf);
    
    this.iterator = scanner.iterator();
  }
  
  public boolean hasNext() {
    return iterator.hasNext();
  }
  
  public Entry<Key,Value> next() {
    mloop: while (true) {
      Entry<Key,Value> entry = iterator.next();
      
      byte[] cf = entry.getKey().getColumnFamilyData().toArray();
      byte[] cq = entry.getKey().getColumnQualifierData().toArray();
      long colType = entry.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;
      
      if (colType == ColumnUtil.LOCK_PREFIX) {
        // TODO exponential back off and eventually do lock recovery
        // TODO do read ahead while waiting for the lock
        UtilWaitThread.sleep(1000);
        
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
        return entry;
      } else {
        throw new IllegalArgumentException();
      }
    }
  }
  
  public void remove() {
    iterator.remove();
  }
  
}
