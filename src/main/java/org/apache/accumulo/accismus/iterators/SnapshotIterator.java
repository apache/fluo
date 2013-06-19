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
package org.apache.accumulo.accismus.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.accismus.impl.ColumnUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

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
      
      if (source.hasTop())
        curCol.set(source.getTopKey());
      
      while (source.hasTop() && curCol.equals(getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
        long colType = source.getTopKey().getTimestamp() & ColumnUtil.PREFIX_MASK;
        long ts = source.getTopKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
        
        if (colType == ColumnUtil.WRITE_PREFIX) {
          
          if (invalidationTime == -1) {
            invalidationTime = ts;
          }
          
          if (dataPointer == -1 && ts <= snaptime) {
            byte[] val = source.getTopValue().get();
            if (val.length == 1 && val[0] == 'D') {
              // found a delete marker, ignore any data for this column
              dataPointer = -2;
            } else if (val.length == 8) {
              dataPointer = (((long) val[0] << 56) + ((long) (val[1] & 255) << 48) + ((long) (val[2] & 255) << 40) + ((long) (val[3] & 255) << 32)
                  + ((long) (val[4] & 255) << 24) + ((val[5] & 255) << 16) + ((val[6] & 255) << 8) + ((val[7] & 255) << 0));
            } else {
              throw new IllegalArgumentException();
            }
          }
        } else if (colType == ColumnUtil.DEL_LOCK_PREFIX) {
          if (ts > invalidationTime)
            invalidationTime = ts;
        } else if (colType == ColumnUtil.LOCK_PREFIX) {
          if (ts > invalidationTime) {
            // nothing supersedes this lock, therefore the column is locked
            return;
          }
        } else if (colType == ColumnUtil.DATA_PREFIX) {
          if (dataPointer == ts) {
            // found data for this column
            return;
          }
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
    if (time < 0 || (ColumnUtil.WRITE_PREFIX & time) != 0) {
      throw new IllegalArgumentException();
    }
    cfg.addOption(TIMESTAMP_OPT, time + "");
  }
}
