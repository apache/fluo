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

package org.apache.fluo.core.worker.finder.hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.accumulo.core.data.Range;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.hadoop.io.Text;

import static java.util.stream.Collectors.toList;

public class TableRange implements Comparable<TableRange> {
  private final Bytes prevEndRow;
  private final Bytes endRow;
  private final int hc;

  public TableRange(Bytes per, Bytes er) {
    this.prevEndRow = per;
    this.endRow = er;
    this.hc = Objects.hash(this.prevEndRow, this.endRow);
  }

  @Override
  public int hashCode() {
    return hc;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TableRange) {
      TableRange ot = (TableRange) o;
      return Objects.equals(prevEndRow, ot.prevEndRow) && Objects.equals(endRow, ot.endRow);
    }

    return false;
  }

  public Bytes getPrevEndRow() {
    return prevEndRow;
  }

  public Bytes getEndRow() {
    return endRow;
  }

  public boolean contains(Bytes row) {
    return (prevEndRow == null || row.compareTo(prevEndRow) > 0)
        && (endRow == null || row.compareTo(endRow) <= 0);
  }

  @Override
  public String toString() {
    return getPrevEndRow() + " " + getEndRow();
  }


  public static Collection<TableRange> toTabletRanges(Collection<Bytes> rows) {
    List<Bytes> sortedRows = rows.stream().sorted().collect(toList());
    List<TableRange> tablets = new ArrayList<>(sortedRows.size() + 1);
    for (int i = 0; i < sortedRows.size(); i++) {
      tablets.add(new TableRange(i == 0 ? null : sortedRows.get(i - 1), sortedRows.get(i)));
    }

    tablets.add(new TableRange(
        sortedRows.size() == 0 ? null : sortedRows.get(sortedRows.size() - 1), null));
    return tablets;
  }



  public Range getRange() {
    Text tper = Optional.ofNullable(prevEndRow).map(ByteUtil::toText).orElse(null);
    Text ter = Optional.ofNullable(endRow).map(ByteUtil::toText).orElse(null);
    return new Range(tper, false, ter, true);
  }

  @Override
  public int compareTo(TableRange o) {
    if (Objects.equals(getEndRow(), o.getEndRow())) {
      // this will catch case of both null
      return 0;
    }

    if (getEndRow() == null) {
      return 1;
    }

    if (o.getEndRow() == null) {
      return -1;
    }

    return getEndRow().compareTo(o.getEndRow());
  }
}
