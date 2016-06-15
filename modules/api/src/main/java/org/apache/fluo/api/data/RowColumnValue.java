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

/**
 * An immutable object that can hold a row, column, and value.
 */

public class RowColumnValue extends RowColumn {
  private static final long serialVersionUID = 1L;

  private Bytes val = Bytes.EMPTY;

  public RowColumnValue(Bytes row, Column col, Bytes val) {
    super(row, col);
    this.val = val;
  }

  /**
   * @param row (will be UTF-8 encoded)
   * @param val (will be UTF-8 encoded)
   */
  public RowColumnValue(String row, Column col, String val) {
    super(Bytes.of(row), col);
    this.val = Bytes.of(val);
  }

  public Bytes getValue() {
    return val;
  }

  public String getsValue() {
    return val.toString();
  }

  @Override
  public int hashCode() {
    return super.hashCode() + 31 * val.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (o instanceof RowColumnValue) {
      RowColumnValue orcv = (RowColumnValue) o;

      if (super.equals(orcv)) {
        return val.equals(orcv.val);
      }
    }
    return false;
  }

  @Override
  public int compareTo(RowColumn orc) {
    if (orc == this) {
      return 0;
    }

    if (!(orc instanceof RowColumnValue)) {
      throw new IllegalArgumentException("Can only compare to same type");
    }

    int result = super.compareTo(orc);
    if (result == 0) {
      RowColumnValue orcv = (RowColumnValue) orc;
      result = val.compareTo(orcv.val);
    }
    return result;
  }

  @Override
  public String toString() {
    return super.toString() + " " + val;
  }
}
