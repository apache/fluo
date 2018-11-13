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

package org.apache.fluo.core.impl.scanner;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.Function;

import com.google.common.collect.Iterators;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.CachedColumnConverter;
import org.apache.fluo.core.util.ColumnUtil;

public class CellScannerImpl implements CellScanner {

  private Iterable<Entry<Key, Value>> snapshot;
  private Function<Key, Column> columnConverter;

  private static final ByteSequence EMPTY_BS = new ArrayByteSequence(new byte[0]);
  private ByteSequence prevRowBs = EMPTY_BS;
  private Bytes prevRowBytes = Bytes.EMPTY;

  private RowColumnValue entry2rcv(Entry<Key, Value> entry) {

    ByteSequence rowBS = entry.getKey().getRowData();
    Bytes row;
    if (prevRowBs.equals(rowBS)) {
      row = prevRowBytes;
    } else {
      prevRowBs = rowBS;
      prevRowBytes = row = ByteUtil.toBytes(rowBS);
    }

    Column col = columnConverter.apply(entry.getKey());
    Bytes val = Bytes.of(entry.getValue().get());
    return new RowColumnValue(row, col, val);
  }

  CellScannerImpl(Iterable<Entry<Key, Value>> snapshot, Collection<Column> columns) {
    this.snapshot = snapshot;
    if (columns.isEmpty()) {
      columnConverter = ColumnUtil::convert;
    } else {
      columnConverter = new CachedColumnConverter(columns);
    }
  }

  @Override
  public Iterator<RowColumnValue> iterator() {
    return Iterators.transform(snapshot.iterator(), this::entry2rcv);
  }
}
