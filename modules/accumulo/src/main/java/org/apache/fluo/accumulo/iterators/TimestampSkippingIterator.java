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
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.system.DeletingIterator;
import org.apache.accumulo.core.iterators.system.SourceSwitchingIterator;
import org.apache.accumulo.core.iterators.system.SynchronizedIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The purpose of this iterator is to make seeking within a columns timestamp range efficient.
 * Accumulo's builtin deleting iterator gets in the way when trying to efficiently do these seeks.
 * Therefore this class attempts to remove that iterator via reflection.
 */

public class TimestampSkippingIterator implements SortedKeyValueIterator<Key, Value> {

  private final SortedKeyValueIterator<Key, Value> source;
  private Range range;
  private Collection<ByteSequence> fams;
  private boolean inclusive;
  private boolean hasSeeked = false;

  private boolean removedDeletingIterator = false;
  private int removalFailures = 0;

  private static final Logger log = LoggerFactory.getLogger(TimestampSkippingIterator.class);

  public TimestampSkippingIterator(SortedKeyValueIterator<Key, Value> source) {
    this.source = source;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasTop() {
    return source.hasTop();
  }

  @Override
  public void next() throws IOException {
    source.next();
  }

  public void skipToTimestamp(Key curCol, long timestamp) throws IOException {
    source.next();
    int count = 0;
    while (source.hasTop()
        && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
        && timestamp < source.getTopKey().getTimestamp()) {
      if (count == 10 && shouldSeek()) {
        // seek to prefix
        Key seekKey = new Key(curCol);
        seekKey.setTimestamp(timestamp);
        Range newRange = new Range(seekKey, true, range.getEndKey(), range.isEndKeyInclusive());
        seek(newRange);
        break;
      }
      source.next();
      count++;
    }
  }

  public void skipToPrefix(Key curCol, long prefix) throws IOException {
    // first possible timestamp in sorted order for this prefix
    long timestamp = prefix | ColumnConstants.TIMESTAMP_MASK;
    skipToTimestamp(curCol, timestamp);
  }

  public void skipColumn(Key curCol) throws IOException {
    source.next();
    int count = 0;
    while (source.hasTop()
        && curCol.equals(source.getTopKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      if (count == 10) {
        Key seekKey = curCol.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
        Range newRange;
        if (range.afterEndKey(seekKey)) {
          // this range will force source.hasTop() to return false, because nothing can exist in the
          // range.
          newRange = new Range(range.getEndKey(), true, range.getEndKey(), false);
        } else {
          newRange = new Range(seekKey, true, range.getEndKey(), range.isEndKeyInclusive());
        }
        seek(newRange);
        break;
      }
      source.next();
      count++;
    }
  }

  @SuppressWarnings("unchecked")
  private static SortedKeyValueIterator<Key, Value> getParent(
      SortedKeyValueIterator<Key, Value> iter) {
    try {
      if (iter instanceof WrappingIterator) {
        Field field = WrappingIterator.class.getDeclaredField("source");
        field.setAccessible(true);
        return (SortedKeyValueIterator<Key, Value>) field.get(iter);
      } else if (iter instanceof SourceSwitchingIterator) {
        // TODO could sync on SSI
        Field field = SourceSwitchingIterator.class.getDeclaredField("iter");
        field.setAccessible(true);
        return (SortedKeyValueIterator<Key, Value>) field.get(iter);
      } else if (iter instanceof SynchronizedIterator) {
        Field field = SynchronizedIterator.class.getDeclaredField("source");
        field.setAccessible(true);
        return (SortedKeyValueIterator<Key, Value>) field.get(iter);
      } else if (iter instanceof SortedMapIterator) {
        return null;
      } else {
        return null;
      }
    } catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {
      log.debug(e.getMessage(), e);
      return null;
    }
  }

  private static boolean setParent(SortedKeyValueIterator<Key, Value> iter,
      SortedKeyValueIterator<Key, Value> newParent) {
    try {
      if (iter instanceof WrappingIterator) {
        Field field = WrappingIterator.class.getDeclaredField("source");
        field.setAccessible(true);
        field.set(iter, newParent);
        return true;
      }
    } catch (NoSuchFieldException | IllegalArgumentException | IllegalAccessException e) {
      log.debug(e.getMessage(), e);
    }

    return false;
  }

  private static boolean removeDeletingIterator(SortedKeyValueIterator<Key, Value> source) {

    SortedKeyValueIterator<Key, Value> prev = source;
    SortedKeyValueIterator<Key, Value> parent = getParent(source);

    while (parent != null && !(parent instanceof DeletingIterator)) {
      prev = parent;
      parent = getParent(parent);
    }

    if (parent != null && parent instanceof DeletingIterator) {
      SortedKeyValueIterator<Key, Value> delParent = getParent(parent);
      if (delParent != null) {
        return setParent(prev, delParent);
      }
    }

    return false;
  }

  @VisibleForTesting
  public final boolean shouldSeek() {
    /*
     * This method is a saftey check to ensure the deleting iterator was removed. If this iterator
     * was not removed for some reason, then the performance of seeking will be O(N^2). In the case
     * where its not removed, it would be better to just scan forward.
     */
    return !hasSeeked || removedDeletingIterator || removalFailures < 3;
  }

  private void seek(Range range) throws IOException {
    if (hasSeeked) {
      // Making assumptions based on how Accumulo currently works. Currently Accumulo does not set
      // up iterators until the 1st seek. Therefore can only remove the deleting iter after the 1st
      // seek. Also, Accumulo may switch data sources and re-setup the deleting iterator, thats why
      // this iterator keeps trying to remove it.
      removedDeletingIterator |= removeDeletingIterator(source);
      if (!removedDeletingIterator) {
        removalFailures++;
      }
    }
    source.seek(range, fams, inclusive);
    hasSeeked = true;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    this.range = range;
    this.fams = columnFamilies;
    this.inclusive = inclusive;
    seek(range);
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
}
