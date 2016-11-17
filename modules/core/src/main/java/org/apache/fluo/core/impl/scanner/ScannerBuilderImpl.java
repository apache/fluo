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
import java.util.Collections;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.client.scanner.RowScannerBuilder;
import org.apache.fluo.api.client.scanner.ScannerBuilder;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.core.impl.SnapshotScanner;
import org.apache.fluo.core.impl.TransactionImpl;

public class ScannerBuilderImpl implements ScannerBuilder {

  private static final Span EMPTY_SPAN = new Span();

  private TransactionImpl tx;
  private Span span = EMPTY_SPAN;
  private Collection<Column> columns = Collections.emptyList();

  public ScannerBuilderImpl(TransactionImpl tx) {
    this.tx = tx;
  }

  @Override
  public ScannerBuilder over(Span span) {
    Objects.requireNonNull(span);
    this.span = span;
    return this;
  }

  private void setColumns(Collection<Column> columns) {
    for (Column column : columns) {
      Preconditions.checkArgument(!column.isVisibilitySet(),
          "Fetching visibility is not currently supported");
    }
    this.columns = columns;
  }

  @Override
  public ScannerBuilder fetch(Collection<Column> columns) {
    Objects.requireNonNull(columns);
    setColumns(ImmutableSet.copyOf(columns));
    return this;
  }

  @Override
  public ScannerBuilder fetch(Column... columns) {
    Objects.requireNonNull(columns);
    setColumns(ImmutableSet.copyOf(columns));
    return this;
  }

  @Override
  public CellScanner build() {
    SnapshotScanner snapScanner = tx.newSnapshotScanner(span, columns);
    return new CellScannerImpl(snapScanner, columns);
  }

  @Override
  public RowScannerBuilder byRow() {
    return new RowScannerBuilder() {
      @Override
      public RowScanner build() {
        SnapshotScanner snapScanner = tx.newSnapshotScanner(span, columns);
        return new RowScannerImpl(snapScanner, columns);
      }
    };
  }
}
