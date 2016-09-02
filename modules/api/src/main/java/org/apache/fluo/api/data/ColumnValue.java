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
 * @since 1.0.0
 */

public final class ColumnValue implements Serializable, Comparable<ColumnValue> {
  private static final long serialVersionUID = 1L;

  private Column column;
  private Bytes val;

  public ColumnValue(Column col, Bytes val) {
    this.column = col;
    this.val = val;
  }

  public ColumnValue(Column col, CharSequence val) {
    this.column = col;
    this.val = Bytes.of(val);
  }

  public Column getColumn() {
    return column;
  }

  public Bytes getValue() {
    return val;
  }

  /**
   * @return value as UTF-8 decoded string
   */
  public String getsValue() {
    return val.toString();
  }

  @Override
  public int compareTo(ColumnValue o) {
    int comp = column.compareTo(o.column);
    if (comp == 0) {
      comp = val.compareTo(o.val);
    }
    return comp;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ColumnValue) {
      ColumnValue ocv = (ColumnValue) o;
      return column.equals(ocv.column) && val.equals(ocv.val);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(column, val);
  }

  @Override
  public String toString() {
    return column + " " + val;
  }
}
