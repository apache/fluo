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

package org.apache.fluo.core.types;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.config.ScannerConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.iterator.RowIterator;
import org.apache.fluo.core.impl.TxStringUtil;

public class MockSnapshotBase implements SnapshotBase {

  final Map<Bytes, Map<Column, Bytes>> getData;

  /**
   * Initializes {@link #getData} using {@link #toRCVM(String...)}
   */
  MockSnapshotBase(String... entries) {
    getData = toRCVM(entries);
  }

  @Override
  public Bytes get(Bytes row, Column column) {
    Map<Column, Bytes> cols = getData.get(row);
    if (cols != null) {
      return cols.get(column);
    }

    return null;
  }

  @Override
  public Map<Column, Bytes> get(Bytes row, Set<Column> columns) {
    Map<Column, Bytes> ret = new HashMap<>();
    Map<Column, Bytes> cols = getData.get(row);
    if (cols != null) {
      for (Column column : columns) {
        Bytes val = cols.get(column);
        if (val != null) {
          ret.put(column, val);
        }
      }
    }
    return ret;
  }

  @Override
  public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns) {

    Map<Bytes, Map<Column, Bytes>> ret = new HashMap<>();

    for (Bytes row : rows) {
      Map<Column, Bytes> colMap = get(row, columns);
      if (colMap != null && colMap.size() > 0) {
        ret.put(row, colMap);
      }
    }

    return ret;
  }

  @Override
  public RowIterator get(ScannerConfiguration config) {
    throw new UnsupportedOperationException();
  }

  /**
   * toRCVM stands for "To Row Column Value Map". This is a convenience function that takes strings
   * of the format {@code <row>,<col fam>:<col qual>[:col vis],
   * <value>} and generates a row, column, value map.
   */
  public static Map<Bytes, Map<Column, Bytes>> toRCVM(String... entries) {
    Map<Bytes, Map<Column, Bytes>> ret = new HashMap<>();

    for (String entry : entries) {
      String[] rcv = entry.split(",");
      if (rcv.length != 3 && !(rcv.length == 2 && entry.trim().endsWith(","))) {
        throw new IllegalArgumentException(
            "expected <row>,<col fam>:<col qual>[:col vis],<value> but saw : " + entry);
      }

      Bytes row = Bytes.of(rcv[0]);
      String[] colFields = rcv[1].split(":");

      Column col;
      if (colFields.length == 3) {
        col = new Column(colFields[0], colFields[1], colFields[2]);
      } else if (colFields.length == 2) {
        col = new Column(colFields[0], colFields[1]);
      } else {
        throw new IllegalArgumentException(
            "expected <row>,<col fam>:<col qual>[:col vis],<value> but saw : " + entry);
      }

      Bytes val;
      if (rcv.length == 2) {
        val = Bytes.EMPTY;
      } else {
        val = Bytes.of(rcv[2]);
      }

      Map<Column, Bytes> cols = ret.get(row);
      if (cols == null) {
        cols = new HashMap<>();
        ret.put(row, cols);
      }

      cols.put(col, val);
    }
    return ret;
  }

  /**
   * toRCM stands for "To Row Column Map". This is a convenience function that takes strings of the
   * format {@code <row>,<col fam>:<col qual>[:col vis]} and generates a row, column map.
   */
  public static Map<Bytes, Set<Column>> toRCM(String... entries) {
    Map<Bytes, Set<Column>> ret = new HashMap<>();

    for (String entry : entries) {
      String[] rcv = entry.split(",");
      if (rcv.length != 2) {
        throw new IllegalArgumentException(
            "expected <row>,<col fam>:<col qual>[:col vis] but saw : " + entry);
      }

      Bytes row = Bytes.of(rcv[0]);
      String[] colFields = rcv[1].split(":");

      Column col;
      if (colFields.length == 3) {
        col = new Column(colFields[0], colFields[1], colFields[2]);
      } else if (colFields.length == 2) {
        col = new Column(colFields[0], colFields[1]);
      } else {
        throw new IllegalArgumentException(
            "expected <row>,<col fam>:<col qual>[:col vis],<value> but saw : " + entry);
      }

      Set<Column> cols = ret.get(row);
      if (cols == null) {
        cols = new HashSet<>();
        ret.put(row, cols);
      }

      cols.add(col);
    }
    return ret;
  }

  @Override
  public long getStartTimestamp() {
    throw new UnsupportedOperationException();
  }


  @Override
  public String gets(String row, Column column) {
    return TxStringUtil.gets(this, row, column);
  }

  @Override
  public Map<Column, String> gets(String row, Set<Column> columns) {
    return TxStringUtil.gets(this, row, columns);
  }

  @Override
  public Map<String, Map<Column, String>> gets(Collection<String> rows, Set<Column> columns) {
    return TxStringUtil.gets(this, rows, columns);
  }

  @Override
  public Map<String, Map<Column, String>> gets(Collection<RowColumn> rowColumns) {
    return TxStringUtil.gets(this, rowColumns);
  }

  @Override
  public Map<Bytes, Map<Column, Bytes>> get(Collection<RowColumn> rowColumns) {
    throw new UnsupportedOperationException();
  }
}
