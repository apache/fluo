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
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import org.fluo.impl.ColumnUtil;
import org.fluo.impl.DelLockValue;
import org.fluo.impl.WriteValue;

/**
 * 
 */
public class RollbackCheckIterator implements SortedKeyValueIterator<Key,Value> {
  private static final String TIMESTAMP_OPT = "timestampOpt";
  
  private SortedKeyValueIterator<Key,Value> source;
  private long lockTime;
  
  boolean hasTop = false;
  boolean checkAck = false;
  
  public static void setLocktime(IteratorSetting cfg, long time) {
    if (time < 0 || (ColumnUtil.PREFIX_MASK & time) != 0) {
      throw new IllegalArgumentException();
    }
    cfg.addOption(TIMESTAMP_OPT, time + "");
  }
  
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    this.source = source;
    this.lockTime = Long.parseLong(options.get(TIMESTAMP_OPT));
  }
  
  public boolean hasTop() {
    return hasTop && source.hasTop();
  }
  
  public void next() throws IOException {
    hasTop = false;
  }
  
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    IteratorUtil.maximizeStartKeyTimeStamp(range);

    source.seek(range, columnFamilies, inclusive);
    
    Key curCol = new Key();
    
    if (source.hasTop()) {
      curCol.set(source.getTopKey());
      
      // TODO can this optimization cause problems?
      if (!curCol.equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
        return;
      }
    }

    long invalidationTime = -1;

    hasTop = false;
    while (source.hasTop() && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      long colType = source.getTopKey().getTimestamp() & ColumnUtil.PREFIX_MASK;
      long ts = source.getTopKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
      
      if (colType == ColumnUtil.TX_DONE_PREFIX) {
        
      } else if (colType == ColumnUtil.WRITE_PREFIX) {
        long timePtr = WriteValue.getTimestamp(source.getTopValue().get());
        
        if (timePtr > invalidationTime)
          invalidationTime = timePtr;
        
        if (lockTime == timePtr) {
          hasTop = true;
          return;
        }
      } else if (colType == ColumnUtil.DEL_LOCK_PREFIX) {
        long timePtr = DelLockValue.getTimestamp(source.getTopValue().get());
        
        if (timePtr > invalidationTime)
          invalidationTime = timePtr;
        
        if (timePtr == lockTime) {
          hasTop = true;
          return;
        }

      } else if (colType == ColumnUtil.LOCK_PREFIX) {
        if (ts > invalidationTime) {
          // nothing supersedes this lock, therefore the column is locked
          hasTop = true;
          return;
        }
      } else if (colType == ColumnUtil.DATA_PREFIX) {
        // can stop looking
        return;
      } else if (colType == ColumnUtil.ACK_PREFIX) {

      } else {
        throw new IllegalArgumentException();
      }
      
      source.next();
    }
  }
  
  public Key getTopKey() {
    return source.getTopKey();
  }
  
  public Value getTopValue() {
    return source.getTopValue();
  }
  
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    // TODO Auto-generated method stub
    return null;
  }
}
