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

package org.apache.fluo.api.data;

import java.io.Serializable;
import java.util.Objects;

/**
 * An immutable object that can hold a row, column, and value.
 *
 * @since 1.0.0
 */
public final class RowColumnValue implements Comparable<RowColumnValue>, Serializable {
  private static final long serialVersionUID = 1L;

  private Bytes row = Bytes.EMPTY;
  private Column col = Column.EMPTY;
  private Bytes val = Bytes.EMPTY;

  public RowColumnValue(Bytes row, Column col, Bytes val) {
    this.row = row;
    this.col = col;
    this.val = val;
  }

  /**
   * @param row (will be UTF-8 encoded)
   * @param val (will be UTF-8 encoded)
   */
  public RowColumnValue(CharSequence row, Column col, CharSequence val) {
    this.row = Bytes.of(row);
    this.col = col;
    this.val = Bytes.of(val);
  }

  /**
   * Retrieves Row in RowColumn
   *
   * @return Row
   */
  public Bytes getRow() {
    return row;
  }

  /**
   * Retrieves Row in RowColumn as a String using UTF-8 encoding.
   *
   * @return Row
   */
  public String getsRow() {
    return row.toString();
  }

  /**
   * Retrieves Column in RowColumn
   *
   * @return Column
   */
  public Column getColumn() {
    return col;
  }

  public Bytes getValue() {
    return val;
  }

  public String getsValue() {
    return val.toString();
  }

  public RowColumn getRowColumn() {
    return new RowColumn(row, col);
  }

  @Override
  public int hashCode() {
    return Objects.hash(row, col, val);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (o instanceof RowColumnValue) {
      RowColumnValue orcv = (RowColumnValue) o;
      return row.equals(orcv.row) && col.equals(orcv.col) && val.equals(orcv.val);
    }
    return false;
  }

  @Override
  public String toString() {
    return getRowColumn() + " " + val;
  }

  @Override
  public int compareTo(RowColumnValue o) {
    int result = row.compareTo(o.row);
    if (result == 0) {
      result = col.compareTo(o.col);
      if (result == 0) {
        result = val.compareTo(o.val);
      }
    }
    return result;
  }
}
