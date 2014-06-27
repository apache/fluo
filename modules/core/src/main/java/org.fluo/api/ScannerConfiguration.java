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
package org.fluo.api;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.util.ArgumentChecker;

/**
 * 
 */
public class ScannerConfiguration implements Cloneable {

  private Range range = new Range();
  // TODO use a set
  private ArrayList<Column> columns = new ArrayList<Column>();
  
  // TODO document that timestamps are ignored

  // TODO remove Accumulo Range from API
  public ScannerConfiguration setRange(Range range) {
    if (range.getStartKey() != null && range.getStartKey().getTimestamp() != Long.MAX_VALUE) {
      throw new IllegalArgumentException("Timestamp is set in start Key");
    }

    if (range.getEndKey() != null && range.getEndKey().getTimestamp() != Long.MAX_VALUE) {
      throw new IllegalArgumentException("Timestamp is set in end Key");
    }

    this.range = range;
    return this;
  }

  public Range getRange() {
    return range;
  }

  public List<Column> getColumns() {
    return Collections.unmodifiableList(columns);
  }

  // TODO document SnapshotIterator


  public ScannerConfiguration fetchColumnFamily(ByteSequence col) {
    ArgumentChecker.notNull(col);
    // TODO causes NPE w/ set, add unit test
    columns.add(new Column(col, null));
    return this;
  }

  public ScannerConfiguration fetchColumn(ByteSequence colFam, ByteSequence colQual) {
    ArgumentChecker.notNull(colFam, colQual);
    columns.add(new Column(colFam, colQual));
    return this;
  }

  public void clearColumns() {
    columns.clear();
  }

  @SuppressWarnings("unchecked")
  public Object clone() throws CloneNotSupportedException {
    ScannerConfiguration sc = (ScannerConfiguration) super.clone();

    sc.columns = (ArrayList<Column>) columns.clone();
    sc.range = range;

    return sc;
  }
}
