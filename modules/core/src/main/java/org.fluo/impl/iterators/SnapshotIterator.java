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
package org.fluo.impl.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import org.fluo.impl.ColumnUtil;
import org.fluo.impl.Constants;
import org.fluo.impl.DelLockValue;
import org.fluo.impl.WriteValue;

/**
 * 
 */
public class SnapshotIterator implements SortedKeyValueIterator<Key,Value> {
  
  private static final String TIMESTAMP_OPT = "timestampOpt";
  
  private SortedKeyValueIterator<Key,Value> source;
  private long snaptime;
  private boolean hasTop = false;
  
  private Key curCol = new Key();
  
  private void findTop() throws IOException {
    while (source.hasTop()) {
      long invalidationTime = -1;
      long dataPointer = -1;
      
      if (source.getTopKey().getColumnFamilyData().equals(Constants.NOTIFY_CF)) {
        source.next();
        continue;
      }

      curCol.set(source.getTopKey());
      
      while (source.hasTop() && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
        long colType = source.getTopKey().getTimestamp() & ColumnUtil.PREFIX_MASK;
        long ts = source.getTopKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
        
        if (colType == ColumnUtil.TX_DONE_PREFIX) {
          
        } else if (colType == ColumnUtil.WRITE_PREFIX) {
          // TODO check of truncated writes
          
          long timePtr = WriteValue.getTimestamp(source.getTopValue().get());
          
          if (timePtr > invalidationTime)
            invalidationTime = timePtr;

          if (dataPointer == -1) {
            if (ts <= snaptime)
              dataPointer = timePtr;
            else if (WriteValue.isTruncated(source.getTopValue().get()))
              return;
          }
        } else if (colType == ColumnUtil.DEL_LOCK_PREFIX) {
          long timePtr = DelLockValue.getTimestamp(source.getTopValue().get());
          
          if (timePtr > invalidationTime)
            invalidationTime = timePtr;
        } else if (colType == ColumnUtil.LOCK_PREFIX) {
          if (ts > invalidationTime && ts <= snaptime) {
            // nothing supersedes this lock, therefore the column is locked
            return;
          }
        } else if (colType == ColumnUtil.DATA_PREFIX) {
          if (dataPointer == ts) {
            // found data for this column
            return;
          }
          
          // TODO could possibly seek to next col when ts < dataPointer
        } else if (colType == ColumnUtil.ACK_PREFIX) {
          
        } else {
          throw new IllegalArgumentException();
        }
        
        // TODO handle case where dataPointer >=0, but no data was found
        source.next();
      }
    }
  }
  
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
    this.snaptime = Long.parseLong(options.get(TIMESTAMP_OPT));
    // TODO could require client to send version as a sanity check
  }
  
  public boolean hasTop() {
    return hasTop && source.hasTop();
  }
  
  public void next() throws IOException {
    Key nextCol = source.getTopKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
    
    // TODO seek (consider data size)
    while (source.hasTop() && source.getTopKey().compareTo(nextCol) < 0) {
      source.next();
    }
    
    findTop();
    
  }
  
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    
    // handle continue case
    hasTop = true;
    if (range.getStartKey() != null && range.getStartKey().getTimestamp() != Long.MAX_VALUE && !range.isStartKeyInclusive()) {
      Key nextCol = range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
      if (range.afterEndKey(nextCol)) {
        hasTop = false;
      } else {
        range = new Range(nextCol, true, range.getEndKey(), range.isEndKeyInclusive());
      }
    }
    
    // TODO could possibly exclude notification locality group
    source.seek(range, columnFamilies, inclusive);
    
    findTop();
  }
  
  public Key getTopKey() {
    return source.getTopKey();
  }
  
  public Value getTopValue() {
    return source.getTopValue();
  }
  
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    // TODO implement
    throw new UnsupportedOperationException();
  }
  
  public static void setSnaptime(IteratorSetting cfg, long time) {
    if (time < 0 || (ColumnUtil.PREFIX_MASK & time) != 0) {
      throw new IllegalArgumentException();
    }
    cfg.addOption(TIMESTAMP_OPT, time + "");
  }
}
