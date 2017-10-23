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

import java.util.ArrayList;
import java.util.function.LongPredicate;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * This class buffers Keys that all have the same row+column.  Internally 
 * it only stores one Key, a list of timestamps and a list of values.  At iteration 
 * time it materializes each Key+Value.
 */
class ColumnBuffer {

  private Key key;
  private ArrayList<Long> timeStamps;
  private ArrayList<Value> values;
  private int size;

  public ColumnBuffer() {

    this.key = null;
    this.timeStamps = new ArrayList<>();
    this.values = new ArrayList<>();
    size = 0;
  }

  /**
   * When empty, the first key added sets the row+column.  After this all keys
   * added must have the same row+column.
   */
  public void add(Key k, Value v) {

    if (key == null) {
      key = k;
      timeStamps.add(k.getTimestamp());
      values.add(v);
      size++;
    } else if (key.compareRow(k.getRow()) == 0
        && key.compareColumnFamily(k.getColumnFamily()) == 0) {
      timeStamps.add(k.getTimestamp());
      values.add(v);
    } else {
      //TODO: what happens here
    }
  }

  /**
   * Clears the dest ColumnBuffer and inserts all entries in dest where the timestamp passes 
   * the timestampTest.
   */
  public void copyTo(ColumnBuffer dest, LongPredicate timestampTest) {
    dest.clear();

    for (int i = 0; i < timeStamps.size(); i++) {
      long time = timeStamps.get(i);
      if (timestampTest.test(time)) {
        dest.add(new Key(key.getRow(), time), values.get(i));
      }
    }
  }

  public void clear() {
    timeStamps = null;
    values = null;
  }

  public int size() {
    return size;
  }

  public Key getKey(int pos) {
    return new Key(key.getRow(), timeStamps.get(pos));
  }

  public Value getValue(int pos) {
    return values.get(pos);
  }
}
