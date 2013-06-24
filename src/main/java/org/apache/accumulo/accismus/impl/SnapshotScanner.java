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

import org.apache.accumulo.accismus.ScannerConfiguration;
import org.apache.accumulo.accismus.iterators.PrewriteIterator;
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
  
  private long startTs;
  private Iterator<Entry<Key,Value>> iterator;
  private ScannerConfiguration config;
  private Connector conn;
  private String table;
  
  private static final byte[] EMPTY = new byte[0];

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
    
    IteratorSetting iterConf = new IteratorSetting(1, SnapshotIterator.class);
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
        // TODO exponential back off and eventually do lock recovery
        // TODO do read ahead while waiting for the lock
        if (firstSeen == -1)
          firstSeen = System.currentTimeMillis();

        UtilWaitThread.sleep(waitTime);
        waitTime = Math.min(MAX_WAIT_TIME, waitTime * 2);
        
        if (System.currentTimeMillis() - firstSeen > ROLLBACK_TIME) {
          attemptRollback(entry);
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
      } else {
        throw new IllegalArgumentException();
      }
    }
  }
  
  private void attemptRollback(Entry<Key,Value> entry) {
    // TODO sanity check on lock timestamp

    List<ByteSequence> fields = ByteUtil.split(new ArrayByteSequence(entry.getValue().get()));
    
    ByteSequence row = fields.get(0);
    ByteSequence fam = fields.get(1);
    ByteSequence qual = fields.get(2);
    ByteSequence vis = fields.get(3);

    ColumnVisibility cv = new ColumnVisibility(vis.toArray());

    // TODO avoid conversions to arrays
    ConditionalMutation delLockMutation = new ConditionalMutation(row.toArray());
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    // TODO cache col vis?
    delLockMutation.addCondition(new Condition(fam, qual).setTimestamp(entry.getKey().getTimestamp()).setIterators(iterConf).setVisibility(cv)
        .setValue(entry.getValue().get()));

    delLockMutation.put(fam.toArray(), qual.toArray(), cv, ColumnUtil.DEL_LOCK_PREFIX | startTs, EMPTY);


    ConditionalWriter cw = new ConditionalWriterImpl(table, conn, new Authorizations());
    if (cw.write(delLockMutation).getStatus() == Status.ACCEPTED) {
      Key k = entry.getKey();
      if (!k.getRowData().equals(row) || !k.getColumnFamilyData().equals(fam) || !k.getColumnQualifierData().equals(qual)
          || !k.getColumnVisibilityData().equals(vis)) {
        
        Mutation mut = new Mutation(k.getRowData().toArray());
        mut.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(), k.getColumnVisibilityParsed(), ColumnUtil.DEL_LOCK_PREFIX | startTs,
            EMPTY);
        
        try {
          BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
          bw.addMutation(mut);
          bw.close();
        } catch (TableNotFoundException e) {
          throw new RuntimeException(e);
        } catch (MutationsRejectedException e) {
          throw new RuntimeException(e);
        }
      }
    } else {
      // TODO check if it was commited
    }

  }

  public void remove() {
    iterator.remove();
  }
  
}
