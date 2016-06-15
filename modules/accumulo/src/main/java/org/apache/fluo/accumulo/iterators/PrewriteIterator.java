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
import java.util.Map;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.values.WriteValue;

/**
 *
 */
public class PrewriteIterator implements SortedKeyValueIterator<Key, Value> {
  private static final String TIMESTAMP_OPT = "timestampOpt";
  private static final String CHECK_ACK_OPT = "checkAckOpt";
  private static final String NTFY_TIMESTAMP_OPT = "ntfyTsOpt";

  private TimestampSkippingIterator source;
  private long snaptime;

  boolean hasTop = false;
  boolean checkAck = false;
  long ntfyTimestamp = -1;

  public static void setSnaptime(IteratorSetting cfg, long time) {
    if (time < 0 || (ColumnConstants.PREFIX_MASK & time) != 0) {
      throw new IllegalArgumentException();
    }
    cfg.addOption(TIMESTAMP_OPT, time + "");
  }

  public static void enableAckCheck(IteratorSetting cfg, long timestamp) {
    cfg.addOption(CHECK_ACK_OPT, "true");
    cfg.addOption(NTFY_TIMESTAMP_OPT, timestamp + "");
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {
    this.source = new TimestampSkippingIterator(source);
    this.snaptime = Long.parseLong(options.get(TIMESTAMP_OPT));
    if (options.containsKey(CHECK_ACK_OPT)) {
      this.checkAck = Boolean.parseBoolean(options.get(CHECK_ACK_OPT));
      this.ntfyTimestamp = Long.parseLong(options.get(NTFY_TIMESTAMP_OPT));
    }
  }

  @Override
  public boolean hasTop() {
    return hasTop && source.hasTop();
  }

  @Override
  public void next() throws IOException {
    hasTop = false;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {

    Collection<ByteSequence> fams;
    if (columnFamilies.isEmpty() && !inclusive) {
      fams = SnapshotIterator.NOTIFY_CF_SET;
      inclusive = false;
    } else {
      fams = columnFamilies;
    }

    Key endKey = new Key(range.getStartKey());
    if (checkAck) {
      endKey.setTimestamp(ColumnConstants.DATA_PREFIX | ColumnConstants.TIMESTAMP_MASK);
    } else {
      endKey.setTimestamp(ColumnConstants.ACK_PREFIX | ColumnConstants.TIMESTAMP_MASK);
    }

    // Tried seeking directly to WRITE_PREFIX, however this did not work well because of how
    // TimestampSkippingIterator currently works. Currently, it can not remove the deleting iterator
    // until after the first seek.
    Range seekRange = new Range(range.getStartKey(), true, endKey, false);

    source.seek(seekRange, fams, inclusive);

    hasTop = false;
    long invalidationTime = -1;

    while (source.hasTop()
        && seekRange.getStartKey().equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {

      long colType = source.getTopKey().getTimestamp() & ColumnConstants.PREFIX_MASK;
      long ts = source.getTopKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

      if (colType == ColumnConstants.TX_DONE_PREFIX) {
        // tried to make 1st seek go to WRITE_PREFIX, but this did not allow the DeleteIterator to
        // be removed from the stack so it was slower.
        source.skipToPrefix(seekRange.getStartKey(), ColumnConstants.WRITE_PREFIX);
      } else if (colType == ColumnConstants.WRITE_PREFIX) {
        long timePtr = WriteValue.getTimestamp(source.getTopValue().get());

        if (timePtr > invalidationTime) {
          invalidationTime = timePtr;
        }

        if (ts >= snaptime) {
          hasTop = true;
          return;
        }

        source.skipToPrefix(seekRange.getStartKey(), ColumnConstants.DEL_LOCK_PREFIX);
      } else if (colType == ColumnConstants.DEL_LOCK_PREFIX) {
        if (ts > invalidationTime) {
          invalidationTime = ts;

          if (ts >= snaptime) {
            hasTop = true;
            return;
          }
        }

        source.skipToPrefix(seekRange.getStartKey(), ColumnConstants.LOCK_PREFIX);
      } else if (colType == ColumnConstants.LOCK_PREFIX) {
        if (ts > invalidationTime) {
          // nothing supersedes this lock, therefore the column is locked
          hasTop = true;
          return;
        }

        if (checkAck) {
          source.skipToPrefix(seekRange.getStartKey(), ColumnConstants.ACK_PREFIX);
        } else {
          // only ack and data left and not interested in either so stop looking
          return;
        }
      } else if (colType == ColumnConstants.DATA_PREFIX) {
        // can stop looking
        return;
      } else if (colType == ColumnConstants.ACK_PREFIX) {
        if (checkAck && ts > ntfyTimestamp) {
          hasTop = true;
          return;
        } else {
          // nothing else to look at in this column
          return;
        }
      } else {
        throw new IllegalArgumentException();
      }
    }
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
    // TODO Auto-generated method stub
    return null;
  }
}
