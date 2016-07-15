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

import java.util.Iterator;
import java.util.Map.Entry;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.core.util.ByteUtil;

public class CellScannerImpl implements CellScanner {

  private Iterable<Entry<Key, Value>> snapshot;

  private static final Function<Entry<Key, Value>, RowColumnValue> E2RCV =
      new Function<Entry<Key, Value>, RowColumnValue>() {
        @Override
        public RowColumnValue apply(Entry<Key, Value> entry) {
          Bytes row = ByteUtil.toBytes(entry.getKey().getRowData());
          Bytes cf = ByteUtil.toBytes(entry.getKey().getColumnFamilyData());
          Bytes cq = ByteUtil.toBytes(entry.getKey().getColumnQualifierData());
          Bytes cv = ByteUtil.toBytes(entry.getKey().getColumnVisibilityData());
          Column col = new Column(cf, cq, cv);
          Bytes val = Bytes.of(entry.getValue().get());
          return new RowColumnValue(row, col, val);
        }
      };

  CellScannerImpl(Iterable<Entry<Key, Value>> snapshot) {
    this.snapshot = snapshot;
  }

  @Override
  public Iterator<RowColumnValue> iterator() {
    return Iterators.transform(snapshot.iterator(), E2RCV);
  }
}
