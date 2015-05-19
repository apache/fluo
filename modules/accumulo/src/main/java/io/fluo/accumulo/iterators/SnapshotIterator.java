/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.accumulo.iterators;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.values.DelLockValue;
import io.fluo.accumulo.values.WriteValue;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class SnapshotIterator implements SortedKeyValueIterator<Key, Value> {

  @VisibleForTesting
  static final String TIMESTAMP_OPT = "timestampOpt";
  private static final ByteSequence NOTIFY_CF_BS = new ArrayByteSequence(
      ColumnConstants.NOTIFY_CF.toArray());

  static final Set<ByteSequence> NOTIFY_CF_SET = Collections.singleton(NOTIFY_CF_BS);

  private SortedKeyValueIterator<Key, Value> source;
  private long snaptime;
  private boolean hasTop = false;

  private final Key curCol = new Key();

  private void findTop() throws IOException {
    while (source.hasTop()) {
      long invalidationTime = -1;
      long dataPointer = -1;

      if (source.getTopKey().getColumnFamilyData().equals(NOTIFY_CF_BS)) {
        source.next();
        continue;
      }

      curCol.set(source.getTopKey());

      while (source.hasTop()
          && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
        long colType = source.getTopKey().getTimestamp() & ColumnConstants.PREFIX_MASK;
        long ts = source.getTopKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

        if (colType == ColumnConstants.TX_DONE_PREFIX) {
          // do nothing if TX_DONE
        } else if (colType == ColumnConstants.WRITE_PREFIX) {
          // TODO check of truncated writes

          long timePtr = WriteValue.getTimestamp(source.getTopValue().get());

          if (timePtr > invalidationTime) {
            invalidationTime = timePtr;
          }

          if (dataPointer == -1) {
            if (ts <= snaptime) {
              dataPointer = timePtr;
            } else if (WriteValue.isTruncated(source.getTopValue().get())) {
              return;
            }
          }
        } else if (colType == ColumnConstants.DEL_LOCK_PREFIX) {
          long timePtr = DelLockValue.getTimestamp(source.getTopValue().get());

          if (timePtr > invalidationTime) {
            invalidationTime = timePtr;
          }
        } else if (colType == ColumnConstants.LOCK_PREFIX) {
          if (ts > invalidationTime && ts <= snaptime) {
            // nothing supersedes this lock, therefore the column is locked
            return;
          }
        } else if (colType == ColumnConstants.DATA_PREFIX) {
          if (dataPointer == ts) {
            // found data for this column
            return;
          }

          // TODO could possibly seek to next col when ts < dataPointer
        } else if (colType == ColumnConstants.ACK_PREFIX) {
          // do nothing if ACK
        } else {
          throw new IllegalArgumentException();
        }

        // TODO handle case where dataPointer >=0, but no data was found
        source.next();
      }
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {
    this.source = source;
    this.snaptime = Long.parseLong(options.get(TIMESTAMP_OPT));
    // TODO could require client to send version as a sanity check
  }

  @Override
  public boolean hasTop() {
    return hasTop && source.hasTop();
  }

  @Override
  public void next() throws IOException {
    Key nextCol = source.getTopKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS);

    // TODO seek (consider data size)
    while (source.hasTop() && source.getTopKey().compareTo(nextCol) < 0) {
      source.next();
    }

    findTop();

  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {

    // handle continue case
    hasTop = true;
    if (range.getStartKey() != null && range.getStartKey().getTimestamp() != Long.MAX_VALUE
        && !range.isStartKeyInclusive()) {
      Key nextCol = range.getStartKey().followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
      if (range.afterEndKey(nextCol)) {
        hasTop = false;
      } else {
        range = new Range(nextCol, true, range.getEndKey(), range.isEndKeyInclusive());
      }
    }

    if (columnFamilies.size() == 0 && inclusive == false) {
      source.seek(range, NOTIFY_CF_SET, false);
    } else {
      source.seek(range, columnFamilies, inclusive);
    }

    findTop();
  }

  @Override
  public Key getTopKey() {
    return source.getTopKey();
  }

  @Override
  public Value getTopValue() {
    return source.getTopValue();
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  public static void setSnaptime(IteratorSetting cfg, long time) {
    if (time < 0 || (ColumnConstants.PREFIX_MASK & time) != 0) {
      throw new IllegalArgumentException();
    }
    cfg.addOption(TIMESTAMP_OPT, time + "");
  }
}
