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

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;

import static org.apache.fluo.accumulo.util.NotificationUtil.isDelete;
import static org.apache.fluo.accumulo.util.NotificationUtil.isNtfy;

/*
 * <p>When persisted, the last bit of the notification indicates if its a delete. This is done so
 * that deletes sort first. Accumulo deletes are not used because notifications after a delete can
 * not be seen and analyzed. This iterator analyzes notifications after a delete marker to determine
 * if a deleter marker should be carried forward by a partial compaction. This is done try to lessen
 * the number of persisted delete markers over time. If Accumulo supported independent compaction of
 * locality groups, then this would not be needed.
 * 
 * <p>This iterator also ensure only one notification per column is returned, similar to what the
 * versioning iterator does. Could not use the versioning iterator, because it can not be applied to
 * a single column family.
 */
public class NotificationIterator extends SkippingIterator {

  public static final ByteSequence NTFY_CF = new ArrayByteSequence(
      ColumnConstants.NOTIFY_CF.toArray());

  private boolean scanOrFullMajc;
  private boolean lastKeySet = false;
  private Key lastKey = new Key();
  private Range seekRange;
  private Collection<ByteSequence> colFams;
  private boolean inclusive;

  private void skipRowCol(PushbackIterator source, Key key) throws IOException {
    int count = 0;
    while (source.hasTop() && source.getTopKey().equals(key, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      if (count == 10) {
        Key nextKey = key.followingKey(PartialKey.ROW_COLFAM_COLQUAL_COLVIS);
        if (!seekRange.afterEndKey(nextKey)) {
          seekRange =
              new Range(nextKey, true, seekRange.getEndKey(), seekRange.isEndKeyInclusive());
          source.seek(seekRange, colFams, inclusive);
          break;
        }
      }
      source.next();
      count++;
    }
  }

  @Override
  protected void consume() throws IOException {
    PushbackIterator source = (PushbackIterator) getSource();
    if (lastKeySet == true) {
      skipRowCol(source, lastKey);
    }

    consumeDeletes(source);

    if (source.hasTop() && isNtfy(source.getTopKey()) && !isDelete(source.getTopKey())) {
      lastKey.set(source.getTopKey());
      lastKeySet = true;
    } else {
      lastKeySet = false;
    }
  }

  private void consumeDeletes(PushbackIterator source) throws IOException {
    while (source.hasTop() && isNtfy(source.getTopKey()) && isDelete(source.getTopKey())) {
      Key keyCopy = new Key(source.getTopKey());
      if (scanOrFullMajc) {
        // consume everything w/ the same row col
        source.next();
        skipRowCol(source, keyCopy);
      } else {
        // need to make a decision about propagating delete... IF the pattern of notifications and
        // deletes seems orderly AND it does not end with a delete THEN
        // do not propagate delete
        Value valCopy = new Value(source.getTopValue());

        boolean lastKeyWasDelete = true;
        boolean isOrderly = true; // used to check that deletes and notifications alternate

        source.next();
        while (source.hasTop()
            && source.getTopKey().equals(keyCopy, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
          isOrderly &= isDelete(source.getTopKey()) ^ lastKeyWasDelete;
          lastKeyWasDelete = isDelete(source.getTopKey());
          source.next();
        }

        if (!isOrderly || lastKeyWasDelete) {
          // dropping this delete on a partial compaction does not look promising, so propagate the
          // delete marker
          source.pushback(keyCopy, valCopy);
          break;
        }
      }
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    lastKeySet = false;

    seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
    this.colFams = new HashSet<>(columnFamilies);
    this.inclusive = inclusive;
    super.seek(seekRange, columnFamilies, inclusive);

    while (hasTop() && range.beforeStartKey(getTopKey())) {
      next();
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {
    scanOrFullMajc =
        env.getIteratorScope() == IteratorScope.scan
            || (env.getIteratorScope() == IteratorScope.majc && env.isFullMajorCompaction());
    super.init(new PushbackIterator(source), options, env);
  }
}
