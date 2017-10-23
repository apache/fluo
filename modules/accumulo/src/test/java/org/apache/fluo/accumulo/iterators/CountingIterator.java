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
import java.util.SortedMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedMapIterator;

public class CountingIterator extends SortedMapIterator {

  public static class Counter {
    public int nextCalls;
    public int seeks;

    public void reset() {
      nextCalls = 0;
      seeks = 0;
    }
  }

  private Counter counter;

  CountingIterator(Counter counter, SortedMap<Key, Value> data) {
    super(data);
    this.counter = counter;
  }

  @Override
  public void next() throws IOException {
    super.next();
    if (counter != null) {
      counter.nextCalls++;
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    super.seek(range, columnFamilies, inclusive);
    if (counter != null) {
      counter.seeks++;
    }
  }
}
