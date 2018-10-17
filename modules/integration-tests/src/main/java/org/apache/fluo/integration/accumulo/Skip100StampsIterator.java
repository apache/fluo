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

package org.apache.fluo.integration.accumulo;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.fluo.accumulo.iterators.TimestampSkippingIterator;

public class Skip100StampsIterator implements SortedKeyValueIterator<Key, Value> {

  private TimestampSkippingIterator source;
  private boolean hasTop;
  private int goodVal;

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {

    this.source = new TimestampSkippingIterator(source);
  }

  @Override
  public boolean hasTop() {
    return hasTop;
  }

  @Override
  public void next() throws IOException {
    hasTop = false;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {

    source.seek(range, columnFamilies, inclusive);

    Key k = new Key("r1", "f1", "q1");

    long ts = 99999;
    goodVal = 0;

    while (source.hasTop() && ts > 0) {
      source.skipToTimestamp(k, ts);
      if (source.hasTop()) {
        if (source.getTopValue().toString().equals("v" + ts)) {
          goodVal++;
        }
      }

      ts -= 100;
    }

    hasTop = goodVal > 0;
  }

  @Override
  public Key getTopKey() {
    return new Key("r1", "f1", "q1", 42);
  }

  @Override
  public Value getTopValue() {
    return new Value("" + goodVal);
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

}
