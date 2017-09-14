/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.accumulo.iterators;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.ZookeeperUtil;
import org.apache.fluo.accumulo.values.DelLockValue;
import org.apache.fluo.accumulo.values.WriteValue;

/**
 * This iterator cleans up old versions and unneeded column metadata. It's intended to be used only
 * at compaction time.
 */
public class GarbageCollectionIterator implements SortedKeyValueIterator<Key, Value> {

  private static class KeyValue extends SimpleImmutableEntry<Key, Value> {
    private static final long serialVersionUID = 1L;

    public KeyValue(Key key, Value value) {
      super(new Key(key), new Value(value));
    }

    public KeyValue(Key key, byte[] value) {
      super(new Key(key), new Value(value));
    }
  }

  @VisibleForTesting
  static final String GC_TIMESTAMP_OPT = "timestamp.gc";

  private static final String ZOOKEEPER_CONNECT_OPT = "zookeeper.connect";
  private static final ByteSequence NOTIFY_CF_BS =
      new ArrayByteSequence(ColumnConstants.NOTIFY_CF.toArray());
  private Long gcTimestamp;
  private SortedKeyValueIterator<Key, Value> source;

  private ArrayList<KeyValue> keys = new ArrayList<>();
  private ArrayList<KeyValue> keysFiltered = new ArrayList<>();
  private HashSet<Long> completeTxs = new HashSet<>();
  private HashSet<Long> rolledback = new HashSet<>();
  private Key curCol = new Key();
  private long truncationTime;
  private int position = 0;
  boolean isFullMajc;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {
    if (env.getIteratorScope() == IteratorScope.scan) {
      throw new IllegalArgumentException();
    }
    this.source = source;

    isFullMajc = env.getIteratorScope() == IteratorScope.majc && env.isFullMajorCompaction();

    String oats = options.get(GC_TIMESTAMP_OPT);
    if (oats != null) {
      gcTimestamp = Long.valueOf(oats);
    } else {
      String zookeepers = options.get(ZOOKEEPER_CONNECT_OPT);
      if (zookeepers == null) {
        throw new IllegalArgumentException("A configuration item for GC iterator was not set");
      }
      gcTimestamp = ZookeeperUtil.getGcTimestamp(zookeepers);
    }
  }

  @Override
  public boolean hasTop() {
    return position < keysFiltered.size() || source.hasTop();
  }

  private void findTop() throws IOException {
    while (source.hasTop()) {
      readColMetadata();
      if (keysFiltered.size() == 0) {
        if (!consumeData()) {
          // not all data for current column was consumed
          break;
        }
      } else {
        break;
      }
    }
  }

  @Override
  public void next() throws IOException {
    if (position < keysFiltered.size() - 1) {
      position++;
      return;
    } else if (position == keysFiltered.size() - 1) {
      position++;
    } else {
      source.next();
    }

    if (consumeData()) {
      findTop();
    }
  }

  /**
   * @return true when all data columns in current column were consumed
   */
  private boolean consumeData() throws IOException {
    while (source.hasTop()
        && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      long colType = source.getTopKey().getTimestamp() & ColumnConstants.PREFIX_MASK;
      long ts = source.getTopKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

      if (colType == ColumnConstants.DATA_PREFIX) {
        if (ts >= truncationTime && !rolledback.contains(ts)) {
          return false;
        }
      } else {
        // TODO check if its a notify
        return false;
      }

      source.next();
    }

    return true;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    source.seek(range, columnFamilies, inclusive);
    findTop();
  }

  private void readColMetadata() throws IOException {

    long invalidationTime = -1;

    boolean oldestSeen = false;
    boolean sawAck = false;
    long firstWrite = -1;

    truncationTime = -1;

    position = 0;
    keys.clear();
    keysFiltered.clear();
    completeTxs.clear();
    rolledback.clear();

    curCol.set(source.getTopKey());

    if (source.getTopKey().getColumnFamilyData().equals(NOTIFY_CF_BS)) {
      return;
    }

    while (source.hasTop()
        && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {

      long colType = source.getTopKey().getTimestamp() & ColumnConstants.PREFIX_MASK;
      long ts = source.getTopKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

      if (colType == ColumnConstants.TX_DONE_PREFIX) {
        keys.add(new KeyValue(source.getTopKey(), source.getTopValue()));
        completeTxs.add(ts);
      } else if (colType == ColumnConstants.WRITE_PREFIX) {
        boolean keep = false;
        boolean complete = completeTxs.contains(ts);
        byte[] val = source.getTopValue().get();
        long timePtr = WriteValue.getTimestamp(val);

        if (WriteValue.isPrimary(val) && !complete) {
          keep = true;
        }

        if (!oldestSeen) {
          if (firstWrite == -1) {
            firstWrite = ts;
          }

          if (ts < gcTimestamp) {
            oldestSeen = true;
            truncationTime = timePtr;
            if (!(WriteValue.isDelete(val) && isFullMajc)) {
              keep = true;
            }
          } else {
            keep = true;
          }
        }

        if (timePtr > invalidationTime) {
          invalidationTime = timePtr;
        }

        if (keep) {
          keys.add(new KeyValue(source.getTopKey(), val));
        } else if (complete) {
          completeTxs.remove(ts);
        }
      } else if (colType == ColumnConstants.DEL_LOCK_PREFIX) {
        boolean keep = false;
        long txDoneTs = DelLockValue.getTxDoneTimestamp(source.getTopValue().get());
        boolean complete = completeTxs.contains(txDoneTs);

        byte[] val = source.getTopValue().get();

        if (!complete && DelLockValue.isPrimary(val)) {
          keep = true;
        }

        if (DelLockValue.isRollback(val)) {
          rolledback.add(ts);
          keep |= !isFullMajc;
        }

        if (ts > invalidationTime) {
          invalidationTime = ts;
        }

        if (keep) {
          keys.add(new KeyValue(source.getTopKey(), source.getTopValue()));
        } else if (complete) {
          completeTxs.remove(txDoneTs);
        }
      } else if (colType == ColumnConstants.LOCK_PREFIX) {
        if (ts > invalidationTime) {
          keys.add(new KeyValue(source.getTopKey(), source.getTopValue()));
        }
      } else if (colType == ColumnConstants.DATA_PREFIX) {
        // can stop looking
        break;
      } else if (colType == ColumnConstants.ACK_PREFIX) {
        if (!sawAck) {
          if (ts >= firstWrite) {
            keys.add(new KeyValue(source.getTopKey(), source.getTopValue()));
          }
          sawAck = true;
        }
      } else {
        throw new IllegalArgumentException(" unknown colType " + String.format("%x", colType));
      }

      source.next();
    }

    for (KeyValue kv : keys) {
      long colType = kv.getKey().getTimestamp() & ColumnConstants.PREFIX_MASK;
      if (colType == ColumnConstants.TX_DONE_PREFIX) {
        if (completeTxs.contains(kv.getKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK)) {
          keysFiltered.add(kv);
        }
      } else {
        keysFiltered.add(kv);
      }
    }
  }

  @Override
  public Key getTopKey() {
    if (position < keysFiltered.size()) {
      return keysFiltered.get(position).getKey();
    } else {
      return source.getTopKey();
    }
  }

  @Override
  public Value getTopValue() {
    if (position < keysFiltered.size()) {
      return keysFiltered.get(position).getValue();
    } else {
      return source.getTopValue();
    }
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  public static void setZookeepers(IteratorSetting gcIter, String zookeepers) {
    gcIter.addOption(ZOOKEEPER_CONNECT_OPT, zookeepers);
  }
}
