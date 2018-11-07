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
import org.apache.fluo.accumulo.util.ColumnType;
import org.apache.fluo.accumulo.util.ReadLockUtil;
import org.apache.fluo.accumulo.util.ZookeeperUtil;
import org.apache.fluo.accumulo.values.DelLockValue;
import org.apache.fluo.accumulo.values.DelReadLockValue;
import org.apache.fluo.accumulo.values.WriteValue;

/**
 * This iterator cleans up old versions and unneeded column metadata. It's intended to be used only
 * at compaction time.
 */
public class GarbageCollectionIterator implements SortedKeyValueIterator<Key, Value> {

  @VisibleForTesting
  static final String GC_TIMESTAMP_OPT = "timestamp.gc";

  private static final String ZOOKEEPER_CONNECT_OPT = "zookeeper.connect";
  private static final ByteSequence NOTIFY_CF_BS =
      new ArrayByteSequence(ColumnConstants.NOTIFY_CF.toArray());
  private Long gcTimestamp;
  private SortedKeyValueIterator<Key, Value> source;

  private ColumnBuffer keys = new ColumnBuffer();
  private ColumnBuffer keysFiltered = new ColumnBuffer();
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
      ColumnType colType = ColumnType.from(source.getTopKey());
      long ts = source.getTopKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

      if (colType == ColumnType.DATA) {
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
    long lastReadLockDeleteTs = -1;

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

    loop: while (source.hasTop()
        && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {

      ColumnType colType = ColumnType.from(source.getTopKey());
      long ts = source.getTopKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

      switch (colType) {
        case TX_DONE: {
          keys.add(source.getTopKey(), source.getTopValue());
          completeTxs.add(ts);
          break;
        }
        case WRITE: {
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
            keys.add(source.getTopKey(), val);
          } else if (complete) {
            completeTxs.remove(ts);
          }
          break;
        }
        case DEL_LOCK: {
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
            keys.add(source.getTopKey(), source.getTopValue());
          } else if (complete) {
            completeTxs.remove(txDoneTs);
          }
          break;
        }
        case RLOCK: {
          boolean keep = false;
          long rlts = ReadLockUtil.decodeTs(ts);
          boolean isDelete = ReadLockUtil.isDelete(ts);

          if (isDelete) {
            lastReadLockDeleteTs = rlts;
          }

          if (rlts > invalidationTime) {
            if (isFullMajc) {
              if (isDelete) {
                if (DelReadLockValue.isRollback(source.getTopValue().get())) {
                  // can drop rolled back read lock delete markers on any full majc, do not need to
                  // consider gcTimestamp
                  keep = false;
                } else {
                  long rlockCommitTs =
                      DelReadLockValue.getCommitTimestamp(source.getTopValue().get());
                  keep = rlockCommitTs >= gcTimestamp;
                }
              } else {
                keep = lastReadLockDeleteTs != rlts;
              }
            } else {
              // can drop deleted read lock entries.. keep the delete entry.
              keep = isDelete || lastReadLockDeleteTs != rlts;
            }
          }

          if (keep) {
            keys.add(source.getTopKey(), source.getTopValue());
          }
          break;
        }
        case LOCK: {
          if (ts > invalidationTime) {
            keys.add(source.getTopKey(), source.getTopValue());
          }
          break;
        }
        case DATA: {
          // can stop looking
          break loop;
        }
        case ACK: {
          if (!sawAck) {
            if (ts >= firstWrite) {
              keys.add(source.getTopKey(), source.getTopValue());
            }
            sawAck = true;
          }
          break;
        }

        default:
          throw new IllegalArgumentException(" unknown colType " + colType);

      }

      source.next();
    }

    keys.copyTo(keysFiltered, (timestamp -> {
      if (ColumnType.from(timestamp) == ColumnType.TX_DONE) {
        return completeTxs.contains(timestamp & ColumnConstants.TIMESTAMP_MASK);
      } else {
        return true;
      }
    }));
  }

  @Override
  public Key getTopKey() {
    if (position < keysFiltered.size()) {
      return keysFiltered.getKey(position);
    } else {
      return source.getTopKey();
    }
  }

  @Override
  public Value getTopValue() {
    if (position < keysFiltered.size()) {
      return keysFiltered.getValue(position);
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
