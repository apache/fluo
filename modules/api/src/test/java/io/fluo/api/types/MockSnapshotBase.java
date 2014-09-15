/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.api.types;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.fluo.api.client.SnapshotBase;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.iterator.RowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;

public class MockSnapshotBase implements SnapshotBase {

  Map<Bytes,Map<Column,Bytes>> getData = new HashMap<>();

  /**
   * Initializes {@link #getData} using {@link #toRCVM(String...)}
   */
  MockSnapshotBase(String... entries) {
    getData = toRCVM(entries);
  }

  @Override
  public Bytes get(Bytes row, Column column) throws Exception {
    Map<Column,Bytes> cols = getData.get(row);
    if (cols != null)
      return cols.get(column);

    return null;
  }

  @Override
  public Map<Column,Bytes> get(Bytes row, Set<Column> columns) throws Exception {
    Map<Column,Bytes> ret = new HashMap<Column,Bytes>();
    Map<Column,Bytes> cols = getData.get(row);
    if (cols != null) {
      for (Column column : columns) {
        Bytes val = cols.get(column);
        if (val != null)
          ret.put(column, val);
      }
    }
    return ret;
  }

  @Override
  public Map<Bytes,Map<Column,Bytes>> get(Collection<Bytes> rows, Set<Column> columns) throws Exception {

    Map<Bytes,Map<Column,Bytes>> ret = new HashMap<>();

    for (Bytes row : rows) {
      Map<Column,Bytes> colMap = get(row, columns);
      if (colMap != null && colMap.size() > 0) {
        ret.put(row, colMap);
      }
    }

    return ret;
  }

  @Override
  public RowIterator get(ScannerConfiguration config) throws Exception {
    throw new UnsupportedOperationException();
  }

  /**
   * toRCVM stands for "To Row Column Value Map". This is a convenience function that takes strings of the format
   * {@code <row>,<col fam>:<col qual>[:col vis],<value>} and generates a row, column, value map.
   */
  public static Map<Bytes,Map<Column,Bytes>> toRCVM(String... entries) {
    Map<Bytes,Map<Column,Bytes>> ret = new HashMap<>();

    for (String entry : entries) {
      String[] rcv = entry.split(",");
      if (rcv.length != 3 && !(rcv.length == 2 && entry.trim().endsWith(",")))
        throw new IllegalArgumentException("expected <row>,<col fam>:<col qual>[:col vis],<value> but saw : " + entry);

      Bytes row = Bytes.wrap(rcv[0]);
      String[] colFields = rcv[1].split(":");
      if (colFields.length != 2 && colFields.length != 3)
        throw new IllegalArgumentException("expected <row>,<col fam>:<col qual>[:col vis],<value> but saw : " + entry);

      Column col = new Column(colFields[0], colFields[1]);
      if (colFields.length == 3)
        col.setVisibility(new ColumnVisibility(colFields[2]));

      Bytes val;
      if (rcv.length == 2)
        val = Bytes.EMPTY;
      else
        val = Bytes.wrap(rcv[2]);

      Map<Column,Bytes> cols = ret.get(row);
      if (cols == null) {
        cols = new HashMap<>();
        ret.put(row, cols);
      }

      cols.put(col, val);
    }
    return ret;
  }

  /**
   * toRCM stands for "To Row Column Map". This is a convenience function that takes strings of the format {@code <row>,<col fam>:<col qual>[:col vis]} and
   * generates a row, column map.
   */
  public static Map<Bytes,Set<Column>> toRCM(String... entries) {
    Map<Bytes,Set<Column>> ret = new HashMap<>();

    for (String entry : entries) {
      String[] rcv = entry.split(",");
      if (rcv.length != 2)
        throw new IllegalArgumentException("expected <row>,<col fam>:<col qual>[:col vis] but saw : " + entry);

      Bytes row = Bytes.wrap(rcv[0]);
      String[] colFields = rcv[1].split(":");
      if (colFields.length != 2 && colFields.length != 3)
        throw new IllegalArgumentException("expected <row>,<col fam>:<col qual>[:col vis] but saw : " + entry);

      Column col = new Column(colFields[0], colFields[1]);
      if (colFields.length == 3)
        col.setVisibility(new ColumnVisibility(colFields[2]));

      Set<Column> cols = ret.get(row);
      if (cols == null) {
        cols = new HashSet<>();
        ret.put(row, cols);
      }

      cols.add(col);
    }
    return ret;
  }
}
