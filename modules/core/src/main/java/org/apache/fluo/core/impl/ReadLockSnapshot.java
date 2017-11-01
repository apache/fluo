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

package org.apache.fluo.core.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.fluo.api.client.AbstractSnapshotBase;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.client.scanner.ScannerBuilder;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;

public class ReadLockSnapshot extends AbstractSnapshotBase implements SnapshotBase {

  private TransactionImpl txi;

  ReadLockSnapshot(TransactionImpl txi) {
    super(txi);
    this.txi = txi;
  }

  @Override
  public Bytes get(Bytes row, Column column) {
    txi.setReadLock(row, column);
    return txi.get(row, column);
  }

  @Override
  public Map<Column, Bytes> get(Bytes row, Set<Column> columns) {
    for (Column column : columns) {
      txi.setReadLock(row, column);
    }
    return txi.get(row, columns);
  }

  @Override
  public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns) {
    for (Bytes row : rows) {
      for (Column column : columns) {
        txi.setReadLock(row, column);
      }
    }
    return txi.get(rows, columns);
  }

  @Override
  public Map<RowColumn, Bytes> get(Collection<RowColumn> rowColumns) {
    for (RowColumn rowColumn : rowColumns) {
      txi.setReadLock(rowColumn.getRow(), rowColumn.getColumn());
    }
    return txi.get(rowColumns);
  }

  @Override
  public ScannerBuilder scanner() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getStartTimestamp() {
    return txi.getStartTimestamp();
  }
}
