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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import org.fluo.impl.ColumnUtil;
import org.fluo.impl.Constants;
import org.fluo.impl.DelLockValue;
import org.fluo.impl.WriteValue;

/**
 * This iterator cleans up old versions and uneeded column metadata. Its intended to be used only at compaction time.
 */
public class GarbageCollectionIterator implements SortedKeyValueIterator<Key,Value> {
  
  // TODO this iterator should support the concept of oldest running scan, and have the ability to gather this from an external source.

  private static final String VERSION_OPT = "numVersions";
  private int numVersions;
  private SortedKeyValueIterator<Key,Value> source;
  
  private ArrayList<KeyValue> keys = new ArrayList<KeyValue>();
  private ArrayList<KeyValue> keysFiltered = new ArrayList<KeyValue>();
  private HashSet<Long> completeTxs = new HashSet<Long>();
  private Key curCol = new Key();
  private long truncationTime;
  private int position = 0;

  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
    if (env.getIteratorScope() == IteratorScope.scan) {
      throw new IllegalArgumentException();
    }
    
    this.source = source;
    this.numVersions = Integer.parseInt(options.get(VERSION_OPT));
  }
  
  public boolean hasTop() {
    return position < keysFiltered.size() || source.hasTop();
  }
  
  public void next() throws IOException {
    if (position < keysFiltered.size() - 1) {
      position++;
      return;
    } else if (position == keysFiltered.size() - 1) {
      position++;
    } else {
      source.next();
    }
    
    while (source.hasTop() && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      long colType = source.getTopKey().getTimestamp() & ColumnUtil.PREFIX_MASK;
      long ts = source.getTopKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
      
      if (colType == ColumnUtil.DATA_PREFIX) {
        if (ts >= truncationTime)
          break;
      } else {
        // TODO check if its a notify
        break;
      }
      
      source.next();
    }

    if (source.hasTop() && !curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      readColMetadata();
    }
  }
  
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
    
    if (source.hasTop()) {
      readColMetadata();
    }
  }
  


  private void readColMetadata() throws IOException {
    
    long invalidationTime = -1;
    
    int writesSeen = 0;
    boolean truncationSeen = false;
    truncationTime = -1;

    position = 0;
    keys.clear();
    keysFiltered.clear();
    completeTxs.clear();

    curCol.set(source.getTopKey());

    if (source.getTopKey().getColumnFamilyData().equals(Constants.NOTIFY_CF)) {
      return;
    }

    while (source.hasTop() && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {

      long colType = source.getTopKey().getTimestamp() & ColumnUtil.PREFIX_MASK;
      long ts = source.getTopKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
      
      if (colType == ColumnUtil.TX_DONE_PREFIX) {
        keys.add(new KeyValue(new Key(source.getTopKey()), source.getTopValue().get()));
        completeTxs.add(ts);
      } else if (colType == ColumnUtil.WRITE_PREFIX) {
        boolean keep = false;
        boolean complete = completeTxs.contains(ts);
        byte[] val = source.getTopValue().get();
        long timePtr = WriteValue.getTimestamp(val);
        
        if (WriteValue.isPrimary(val) && !complete)
          keep = true;
        
        if (writesSeen < numVersions && !truncationSeen) {
          keep = true;
          
          if (WriteValue.isTruncated(val))
            truncationSeen = true;
          
          if (writesSeen == numVersions - 1 || truncationSeen) {
            if (truncationTime != -1)
              throw new IllegalStateException();
            
            truncationTime = timePtr;
            val = WriteValue.encode(truncationTime, WriteValue.isPrimary(val), true);
          }
        }

        if (timePtr > invalidationTime)
          invalidationTime = timePtr;
        
        if (keep) {
          keys.add(new KeyValue(new Key(source.getTopKey()), val));
        } else if (complete) {
          completeTxs.remove(ts);
        }
        
        writesSeen++;

      } else if (colType == ColumnUtil.DEL_LOCK_PREFIX) {
        boolean keep = false;
        boolean complete = completeTxs.contains(ts);
        
        if (DelLockValue.isPrimary(source.getTopValue().get()) && !complete)
          keep = true;

        long timePtr = DelLockValue.getTimestamp(source.getTopValue().get());

        if (timePtr > invalidationTime) {
          invalidationTime = ts;
          keep = true;
        }
        
        if (keep) {
          keys.add(new KeyValue(new Key(source.getTopKey()), source.getTopValue().get()));
        } else if (complete) {
          completeTxs.remove(ts);
        }
      } else if (colType == ColumnUtil.LOCK_PREFIX) {
        if (ts > invalidationTime)
          keys.add(new KeyValue(new Key(source.getTopKey()), source.getTopValue().get()));
      } else if (colType == ColumnUtil.DATA_PREFIX) {
        // can stop looking
        break;
      } else if (colType == ColumnUtil.ACK_PREFIX) {
        // TODO determine which acks can be dropped
        keys.add(new KeyValue(new Key(source.getTopKey()), source.getTopValue().get()));
      } else {
        throw new IllegalArgumentException(" unknown colType " + String.format("%x", colType));
      }
      
      source.next();
    }

    for (KeyValue kv : keys) {
      long colType = kv.key.getTimestamp() & ColumnUtil.PREFIX_MASK;
      if (colType == ColumnUtil.TX_DONE_PREFIX) {
        if (completeTxs.contains(kv.key.getTimestamp() & ColumnUtil.TIMESTAMP_MASK)) {
          keysFiltered.add(kv);
        }
      } else {
        keysFiltered.add(kv);
      }
    }

    if (keysFiltered.size() == 0 && source.hasTop() && !curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      throw new IllegalStateException();
    }
  }

  public Key getTopKey() {
    if (position < keysFiltered.size()) {
      return keysFiltered.get(position).key;
    } else {
      return source.getTopKey();
    }
  }
  
  public Value getTopValue() {
    if (position < keysFiltered.size()) {
      return new Value(keysFiltered.get(position).value);
    } else {
      return source.getTopValue();
    }
  }
  
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException();
  }
  
  public static void setNumVersions(IteratorSetting gcIter, int nv) {
    gcIter.addOption(VERSION_OPT, nv + "");
  }
  
}
