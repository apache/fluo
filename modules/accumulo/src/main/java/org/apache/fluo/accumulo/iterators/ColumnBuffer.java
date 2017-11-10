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
import java.util.Arrays;
import java.util.function.LongPredicate;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;

/**
 * This class buffers Keys that all have the same row+column. Internally it only stores one Key, a
 * list of timestamps and a list of values. At iteration time it materializes each Key+Value.
 */
class ColumnBuffer {

  private Key key;
  private ArrayList<Long> timeStamps;
  private ArrayList<byte[]> values;

  public ColumnBuffer() {

    this.key = null;
    this.timeStamps = new ArrayList<>();
    this.values = new ArrayList<>();
  }

  /**
   * @param timestamp Timestamp to be added to buffer
   * @param v Value to be added to buffer
   */
  private void add(long timestamp, byte[] v) {

    timeStamps.add(timestamp);
    values.add(v);
  }

  /**
   * When empty, the first key added sets the row+column. After this all keys added must have the
   * same row+column.
   *
   * @param k Key to be added to buffer
   * @param vByte Value to be added to buffer
   */
  public void add(Key k, byte[] vByte) throws IllegalArgumentException {
    vByte = Arrays.copyOf(vByte, vByte.length);

    if (key == null) {
      key = new Key(k);
      add(k.getTimestamp(), vByte);
    } else if (key.equals(k, PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
      add(k.getTimestamp(), vByte);
    } else {
      throw new IllegalArgumentException();
    }
  }

  /**
   * When empty, the first key added sets the row+column. After this all keys added must have the
   * same row+column.
   *
   * @param k Key to be added to buffer
   * @param v Value to be added to buffer
   */
  public void add(Key k, Value v) throws IllegalArgumentException {
    add(k, v.get());
  }

  /**
   * Clears the dest ColumnBuffer and inserts all entries in dest where the timestamp passes the
   * timestampTest.
   *
   * @param dest Destination ColumnBuffer
   * @param timestampTest Test to determine which timestamps get added to dest
   */
  public void copyTo(ColumnBuffer dest, LongPredicate timestampTest) {
    dest.clear();

    if (key != null) {
      dest.key = new Key(key);
    }

    for (int i = 0; i < timeStamps.size(); i++) {
      long time = timeStamps.get(i);
      if (timestampTest.test(time)) {
        dest.add(time, values.get(i));
      }
    }
  }

  public void clear() {
    timeStamps.clear();
    values.clear();
    key = null;
  }

  /**
   * @return the size of the current buffer
   */
  public int size() {
    return timeStamps.size();
  }

  /**
   * @param pos Position of the Key that will be retrieved
   * @return The key at a given position
   */
  public Key getKey(int pos) {
    Key tmpKey = new Key(key);
    tmpKey.setTimestamp(timeStamps.get(pos));
    return tmpKey;
  }

  /**
   * @param pos Position of the Value that will be retrieved
   * @return The value at a given position
   */
  public Value getValue(int pos) {
    return new Value(values.get(pos));
  }
}
