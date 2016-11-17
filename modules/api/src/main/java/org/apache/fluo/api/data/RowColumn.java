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
 * Represents all or subset of a Fluo row and {@link Column}. RowColumn is similar to an Accumulo
 * Key. RowColumn is immutable after it is created.
 *
 * @since 1.0.0
 */
public final class RowColumn implements Comparable<RowColumn>, Serializable {

  private static final long serialVersionUID = 1L;
  public static RowColumn EMPTY = new RowColumn();

  private Bytes row = Bytes.EMPTY;
  private Column col = Column.EMPTY;
  private int hashCode = 0;

  /**
   * Constructs a RowColumn with row set to Bytes.EMPTY and column set to Column.EMPTY
   */
  public RowColumn() {}

  /**
   * Constructs a RowColumn with only a row. Column will be set to Column.EMPTY
   *
   * @param row Bytes Row
   */
  public RowColumn(Bytes row) {
    Objects.requireNonNull(row, "Row must not be null");
    this.row = row;
  }

  /**
   * Constructs a RowColumn with only a row. Column will be set to Column.EMPTY
   *
   * @param row (will be UTF-8 encoded)
   */
  public RowColumn(CharSequence row) {
    this(Bytes.of(row));
  }

  /**
   * Constructs a RowColumn
   *
   * @param row Bytes Row
   * @param col Column
   */
  public RowColumn(Bytes row, Column col) {
    Objects.requireNonNull(row, "Row must not be null");
    Objects.requireNonNull(col, "Col must not be null");
    this.row = row;
    this.col = col;
  }

  /**
   * Constructs a RowColumn
   *
   * @param row Row String (will be UTF-8 encoded)
   * @param col Column
   */
  public RowColumn(CharSequence row, Column col) {
    this(Bytes.of(row), col);
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

  @Override
  public String toString() {
    return row + " " + col;
  }

  @Override
  public boolean equals(Object o) {

    if (this == o) {
      return true;
    }

    if (o instanceof RowColumn) {
      RowColumn other = (RowColumn) o;
      return row.equals(other.row) && col.equals(other.col);
    }
    return false;
  }

  @Override
  public int hashCode() {
    if (hashCode == 0) {
      hashCode = Objects.hash(row, col);
    }

    return hashCode;
  }

  /**
   * Returns a RowColumn following the current one
   *
   * @return RowColumn following this one
   */
  public RowColumn following() {
    if (row.equals(Bytes.EMPTY)) {
      return RowColumn.EMPTY;
    } else if (col.equals(Column.EMPTY)) {
      return new RowColumn(followingBytes(row));
    } else if (!col.isQualifierSet()) {
      return new RowColumn(row, new Column(followingBytes(col.getFamily())));
    } else if (!col.isVisibilitySet()) {
      return new RowColumn(row, new Column(col.getFamily(), followingBytes(col.getQualifier())));
    } else {
      return new RowColumn(row, new Column(col.getFamily(), col.getQualifier(),
          followingBytes(col.getVisibility())));
    }
  }

  private byte[] followingArray(byte[] ba) {
    byte[] fba = new byte[ba.length + 1];
    System.arraycopy(ba, 0, fba, 0, ba.length);
    fba[ba.length] = (byte) 0x00;
    return fba;
  }

  private Bytes followingBytes(Bytes b) {
    return Bytes.of(followingArray(b.toArray()));
  }

  @Override
  public int compareTo(RowColumn other) {

    if (this == other) {
      return 0;
    }

    int result = row.compareTo(other.row);
    if (result == 0) {
      result = col.compareTo(other.col);
    }
    return result;
  }
}
