/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.accumulo.iterators;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.util.ZookeeperUtil;
import io.fluo.accumulo.values.DelLockValue;
import io.fluo.accumulo.values.WriteValue;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * This iterator cleans up old versions and unneeded column metadata. 
 * It's intended to be used only at compaction time.
 */
public class GarbageCollectionIterator implements SortedKeyValueIterator<Key,Value> {

  private static final String ZOOKEEPER_CONNECT_OPT = "zookeeper.connect";
  private static final String ZOOKEEPER_ROOT_OPT = "zookeeper.root";
  private static final ByteSequence NOTIFY_CF_BS = new ArrayByteSequence(ColumnConstants.NOTIFY_CF.toArray());
  private Long oldestActiveTs;
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
    String zookeepers = options.get(ZOOKEEPER_CONNECT_OPT);
    String zkRoot = options.get(ZOOKEEPER_ROOT_OPT);
    if (zookeepers == null || zkRoot == null) {
      throw new IllegalArgumentException("A configuration item for GC iterator was not set");
    }
    oldestActiveTs = ZookeeperUtil.getOldestTimestamp(zookeepers, zkRoot);
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
      long colType = source.getTopKey().getTimestamp() & ColumnConstants.PREFIX_MASK;
      long ts = source.getTopKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

      if (colType == ColumnConstants.DATA_PREFIX) {
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

    boolean truncationSeen = false;
    boolean oldestSeen = false;
    truncationTime = -1;

    position = 0;
    keys.clear();
    keysFiltered.clear();
    completeTxs.clear();

    curCol.set(source.getTopKey());

    if (source.getTopKey().getColumnFamilyData().equals(NOTIFY_CF_BS)) {
      return;
    }

    while (source.hasTop() && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {

      long colType = source.getTopKey().getTimestamp() & ColumnConstants.PREFIX_MASK;
      long ts = source.getTopKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

      if (colType == ColumnConstants.TX_DONE_PREFIX) {
        keys.add(new KeyValue(new Key(source.getTopKey()), source.getTopValue().get()));
        completeTxs.add(ts);
      } else if (colType == ColumnConstants.WRITE_PREFIX) {
        boolean keep = false;
        boolean complete = completeTxs.contains(ts);
        byte[] val = source.getTopValue().get();
        long timePtr = WriteValue.getTimestamp(val);

        if (WriteValue.isPrimary(val) && !complete)
          keep = true;
        
        if (!oldestSeen && !truncationSeen) {
          keep = true;
          
          if (timePtr < oldestActiveTs)
            oldestSeen = true;

          if (WriteValue.isTruncated(val))
            truncationSeen = true;

          if (oldestSeen || truncationSeen) {
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
      } else if (colType == ColumnConstants.DEL_LOCK_PREFIX) {
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
      } else if (colType == ColumnConstants.LOCK_PREFIX) {
        if (ts > invalidationTime)
          keys.add(new KeyValue(new Key(source.getTopKey()), source.getTopValue().get()));
      } else if (colType == ColumnConstants.DATA_PREFIX) {
        // can stop looking
        break;
      } else if (colType == ColumnConstants.ACK_PREFIX) {
        // TODO determine which acks can be dropped
        keys.add(new KeyValue(new Key(source.getTopKey()), source.getTopValue().get()));
      } else {
        throw new IllegalArgumentException(" unknown colType " + String.format("%x", colType));
      }

      source.next();
    }

    for (KeyValue kv : keys) {
      long colType = kv.key.getTimestamp() & ColumnConstants.PREFIX_MASK;
      if (colType == ColumnConstants.TX_DONE_PREFIX) {
        if (completeTxs.contains(kv.key.getTimestamp() & ColumnConstants.TIMESTAMP_MASK)) {
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
  
  public static void setZookeepers(IteratorSetting gcIter, String zookeepers) {
    gcIter.addOption(ZOOKEEPER_CONNECT_OPT, zookeepers);
  }
  
  public static void setZookeeperRoot(IteratorSetting gcIter, String zkRoot) {
    gcIter.addOption(ZOOKEEPER_ROOT_OPT, zkRoot);
  }
}
