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
package org.apache.accumulo.accismus;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * 
 */
public class RowIterator implements Iterator<Entry<ByteSequence,ColumnIterator>> {
  
  private org.apache.accumulo.core.client.RowIterator rowIter;

  RowIterator(Iterator<Entry<Key,Value>> scanner) {
    rowIter = new org.apache.accumulo.core.client.RowIterator(scanner);
  }
  
  public boolean hasNext() {
    return rowIter.hasNext();
  }
  
  public Entry<ByteSequence,ColumnIterator> next() {
    Iterator<Entry<Key,Value>> cols = rowIter.next();
    
    Entry<Key,Value> entry = cols.next();

    final ByteSequence row = entry.getKey().getRowData();
    final ColumnIterator coliter = new ColumnIterator(entry, cols);
    
    return new Entry<ByteSequence,ColumnIterator>() {
      
      public ByteSequence getKey() {
        return row;
      }
      
      public ColumnIterator getValue() {
        return coliter;
      }

      public ColumnIterator setValue(ColumnIterator value) {
        throw new UnsupportedOperationException();
      }
    };

  }
  
  public void remove() {
    rowIter.remove();
  }
  
}
