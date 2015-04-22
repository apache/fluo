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

package io.fluo.core.impl;

import java.util.Iterator;
import java.util.Map.Entry;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.core.util.ByteUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Implementation of Column Iterator
 */
public class ColumnIteratorImpl implements ColumnIterator {

  private Iterator<Entry<Key, Value>> scanner;
  private Entry<Key, Value> firstEntry;

  ColumnIteratorImpl(Iterator<Entry<Key, Value>> scanner) {
    this(null, scanner);
  }

  ColumnIteratorImpl(Entry<Key, Value> firstEntry, Iterator<Entry<Key, Value>> cols) {
    this.firstEntry = firstEntry;
    this.scanner = cols;
  }

  @Override
  public boolean hasNext() {
    return firstEntry != null || scanner.hasNext();
  }

  // TODO create custom class to return instead of entry
  @Override
  public Entry<Column, Bytes> next() {
    Entry<Key, Value> entry;
    if (firstEntry != null) {
      entry = firstEntry;
      firstEntry = null;
    } else {
      entry = scanner.next();
    }
    final Bytes cf = ByteUtil.toBytes(entry.getKey().getColumnFamilyData());
    final Bytes cq = ByteUtil.toBytes(entry.getKey().getColumnQualifierData());
    final Bytes cv = ByteUtil.toBytes(entry.getKey().getColumnVisibilityData());

    final Column col = new Column(cf, cq, cv);
    final Bytes val = Bytes.of(entry.getValue().get());

    return new Entry<Column, Bytes>() {

      @Override
      public Bytes setValue(Bytes value) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Bytes getValue() {
        return val;
      }

      @Override
      public Column getKey() {
        return col;
      }
    };
  }

  @Override
  public void remove() {
    scanner.remove();
  }
}
