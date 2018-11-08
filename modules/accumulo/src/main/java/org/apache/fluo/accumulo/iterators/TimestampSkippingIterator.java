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

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.fluo.accumulo.util.ColumnType;

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
      if (count == 10) {
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

  public void skipToPrefix(Key curCol, ColumnType colType) throws IOException {
    skipToTimestamp(curCol, colType.first());
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

  private void seek(Range range) throws IOException {
    source.seek(range, fams, inclusive);
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
