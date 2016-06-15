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
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class PushbackIterator implements SortedKeyValueIterator<Key, Value> {

  private SortedKeyValueIterator<Key, Value> source;
  private Key pushedKey = null;
  private Value pushedValue = null;

  public PushbackIterator(SortedKeyValueIterator<Key, Value> source) {
    this.source = source;
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean hasTop() {
    return pushedKey != null || source.hasTop();
  }

  @Override
  public void next() throws IOException {
    if (pushedKey != null) {
      pushedKey = null;
      pushedValue = null;
    } else {
      source.next();
    }

  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    pushedKey = null;
    pushedValue = null;

    source.seek(range, columnFamilies, inclusive);
  }

  @Override
  public Key getTopKey() {
    if (pushedKey != null) {
      return pushedKey;
    }
    return source.getTopKey();
  }

  @Override
  public Value getTopValue() {
    if (pushedKey != null) {
      return pushedValue;
    }
    return source.getTopValue();
  }

  @Override
  public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException();
  }

  public void pushback(Key key, Value val) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(val);
    Preconditions.checkState(pushedKey == null);
    Preconditions.checkState(pushedValue == null);
    if (source.hasTop()) {
      Preconditions.checkArgument(source.getTopKey().compareTo(key) >= 0);
    }
    pushedKey = key;
    pushedValue = val;
  }
}
