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
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

public class TimeSkippingIteratorTest {
  @Test
  public void testSkipColBug() throws IOException {
    // test for bug fluo#656

    SortedMap<Key, Value> data = new TreeMap<>();

    for (int q = 0; q < 2; q++) {
      for (int i = 0; i < 1000; i++) {
        Key k = new Key("r1", "f1", "q" + q, i);
        Value v = new Value(("" + i).getBytes());

        data.put(k, v);
      }
    }


    SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(data);
    TimestampSkippingIterator tsi = new TimestampSkippingIterator(source);

    Key start = new Key("r1", "f1", "q0", Long.MAX_VALUE);
    Key end = new Key("r1", "f1", "q0", Long.MIN_VALUE);

    tsi.seek(new Range(start, true, end, true), Collections.<ByteSequence>emptySet(), false);

    Key curCol = new Key(tsi.getTopKey());
    Assert.assertTrue(tsi.hasTop());
    Assert.assertEquals(new Key("r1", "f1", "q0", 999), curCol);
    tsi.skipColumn(curCol);

    Assert.assertFalse(tsi.hasTop());

    // make sure fix didn't break anything
    start = new Key("r1", "f1", "q0", Long.MAX_VALUE);
    end = new Key("r1", "f1", "q1", Long.MIN_VALUE);
    tsi.seek(new Range(start, true, end, true), Collections.<ByteSequence>emptySet(), false);
    Assert.assertTrue(tsi.hasTop());
    Assert.assertEquals(new Key("r1", "f1", "q0", 999), tsi.getTopKey());
    tsi.skipColumn(curCol);
    Assert.assertTrue(tsi.hasTop());
    Assert.assertEquals(new Key("r1", "f1", "q1", 999), tsi.getTopKey());
    curCol = new Key(tsi.getTopKey());
    tsi.skipColumn(curCol);
    Assert.assertFalse(tsi.hasTop());

  }
}
