/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Implementation of Column Iterator
 */
public class ColumnIteratorImpl implements ColumnIterator {

  private Iterator<Entry<Key,Value>> scanner;
  private Entry<Key,Value> firstEntry;

  ColumnIteratorImpl(Iterator<Entry<Key,Value>> scanner) {
    this(null, scanner);
  }
  
  ColumnIteratorImpl(Entry<Key,Value> firstEntry, Iterator<Entry<Key,Value>> cols) {
    this.firstEntry = firstEntry;
    this.scanner = cols;
  }

  public boolean hasNext() {
    return firstEntry != null || scanner.hasNext();
  }
  
  // TODO create custom class to return instead of entry
  public Entry<Column,Bytes> next() {
    Entry<Key,Value> entry;
    if (firstEntry != null) {
      entry = firstEntry;
      firstEntry = null;
    } else {
      entry = scanner.next();
    }
    Bytes cf = ByteUtil.toBytes(entry.getKey().getColumnFamilyData());
    Bytes cq = ByteUtil.toBytes(entry.getKey().getColumnQualifierData());
    // TODO cache colvis, pass cache in
    ColumnVisibility cv = entry.getKey().getColumnVisibilityParsed();
    
    final Column col = new Column(cf, cq).setVisibility(cv);
    final Bytes val = Bytes.wrap(entry.getValue().get());

    return new Entry<Column,Bytes>() {
      
      public Bytes setValue(Bytes value) {
        throw new UnsupportedOperationException();
      }
      
      public Bytes getValue() {
        return val;
      }
      
      public Column getKey() {
        return col;
      }
    };
  }
  
  public void remove() {
    scanner.remove();
  }
  
}
