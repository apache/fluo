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

package org.apache.fluo.core.util;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.hadoop.io.Text;

/**
 * Utility methods for manipulating Spans
 */
public class SpanUtil {

  private SpanUtil() {}

  /**
   * Converts a Fluo Span to Accumulo Range
   * 
   * @param span Span
   * @return Range
   */
  public static Range toRange(Span span) {
    return new Range(toKey(span.getStart()), span.isStartInclusive(), toKey(span.getEnd()),
        span.isEndInclusive());
  }

  /**
   * Converts from a Fluo RowColumn to a Accumulo Key
   * 
   * @param rc RowColumn
   * @return Key
   */
  public static Key toKey(RowColumn rc) {
    if ((rc == null) || (rc.getRow().equals(Bytes.EMPTY))) {
      return null;
    }
    Text row = ByteUtil.toText(rc.getRow());
    if ((rc.getColumn().equals(Column.EMPTY)) || !rc.getColumn().isFamilySet()) {
      return new Key(row);
    }
    Text cf = ByteUtil.toText(rc.getColumn().getFamily());
    if (!rc.getColumn().isQualifierSet()) {
      return new Key(row, cf);
    }
    Text cq = ByteUtil.toText(rc.getColumn().getQualifier());
    if (!rc.getColumn().isVisibilitySet()) {
      return new Key(row, cf, cq);
    }
    Text cv = ByteUtil.toText(rc.getColumn().getVisibility());
    return new Key(row, cf, cq, cv);
  }

  /**
   * Converts an Accumulo Range to a Fluo Span
   * 
   * @param range Range
   * @return Span
   */
  public static Span toSpan(Range range) {
    return new Span(toRowColumn(range.getStartKey()), range.isStartKeyInclusive(),
        toRowColumn(range.getEndKey()), range.isEndKeyInclusive());
  }

  /**
   * Converts from an Accumulo Key to a Fluo RowColumn
   * 
   * @param key Key
   * @return RowColumn
   */
  public static RowColumn toRowColumn(Key key) {
    if (key == null) {
      return RowColumn.EMPTY;
    }
    if ((key.getRow() == null) || key.getRow().getLength() == 0) {
      return RowColumn.EMPTY;
    }
    Bytes row = ByteUtil.toBytes(key.getRow());
    if ((key.getColumnFamily() == null) || key.getColumnFamily().getLength() == 0) {
      return new RowColumn(row);
    }
    Bytes cf = ByteUtil.toBytes(key.getColumnFamily());
    if ((key.getColumnQualifier() == null) || key.getColumnQualifier().getLength() == 0) {
      return new RowColumn(row, new Column(cf));
    }
    Bytes cq = ByteUtil.toBytes(key.getColumnQualifier());
    if ((key.getColumnVisibility() == null) || key.getColumnVisibility().getLength() == 0) {
      return new RowColumn(row, new Column(cf, cq));
    }
    Bytes cv = ByteUtil.toBytes(key.getColumnVisibility());
    return new RowColumn(row, new Column(cf, cq, cv));
  }
}
