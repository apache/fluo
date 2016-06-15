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

package org.apache.fluo.core.impl;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.iterator.ColumnIterator;
import org.apache.fluo.api.iterator.RowIterator;

/**
 * Implementation of RowIterator
 */
public class RowIteratorImpl implements RowIterator {

  private final org.apache.accumulo.core.client.RowIterator rowIter;

  RowIteratorImpl(Iterator<Entry<Key, Value>> scanner) {
    rowIter = new org.apache.accumulo.core.client.RowIterator(scanner);
  }

  @Override
  public boolean hasNext() {
    return rowIter.hasNext();
  }

  // TODO create custom class to return instead of entry
  @Override
  public Entry<Bytes, ColumnIterator> next() {
    Iterator<Entry<Key, Value>> cols = rowIter.next();

    Entry<Key, Value> entry = cols.next();

    final Bytes row = Bytes.of(entry.getKey().getRowData().toArray());
    final ColumnIterator coliter = new ColumnIteratorImpl(entry, cols);

    return new Entry<Bytes, ColumnIterator>() {

      @Override
      public Bytes getKey() {
        return row;
      }

      @Override
      public ColumnIterator getValue() {
        return coliter;
      }

      @Override
      public ColumnIterator setValue(ColumnIterator value) {
        throw new UnsupportedOperationException();
      }
    };

  }

  @Override
  public void remove() {
    rowIter.remove();
  }
}
