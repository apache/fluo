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

package org.apache.fluo.core.log;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

import com.google.common.collect.ImmutableSet;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.client.scanner.RowScannerBuilder;
import org.apache.fluo.api.client.scanner.ScannerBuilder;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.core.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TracingScannerBuilder implements ScannerBuilder {

  private static final Logger log = LoggerFactory.getLogger(FluoConfiguration.TRANSACTION_PREFIX);

  private static final Span EMPTY_SPAN = new Span();

  private Span span = EMPTY_SPAN;
  private Collection<Column> columns = Collections.emptyList();

  private final ScannerBuilder wrappedBuilder;
  private final long txid;

  TracingScannerBuilder(ScannerBuilder wrappedBuilder, long txid) {
    this.wrappedBuilder = wrappedBuilder;
    this.txid = txid;
  }

  @Override
  public ScannerBuilder over(Span span) {
    Objects.requireNonNull(span);
    this.span = span;
    wrappedBuilder.over(span);
    return this;
  }

  @Override
  public ScannerBuilder fetch(Column... columns) {
    Objects.requireNonNull(columns);
    this.columns = ImmutableSet.copyOf(columns);
    wrappedBuilder.fetch(this.columns);
    return this;
  }

  @Override
  public ScannerBuilder fetch(Collection<Column> columns) {
    Objects.requireNonNull(columns);
    this.columns = ImmutableSet.copyOf(columns);
    wrappedBuilder.fetch(this.columns);
    return this;
  }

  @Override
  public CellScanner build() {
    String scanId = Integer.toHexString(Math.abs(Objects.hash(span, columns, txid)));
    log.trace("txid: {} scanId: {} scanner().over({}).fetch({}).build()", txid, scanId,
        Hex.encNonAscii(span), Hex.encNonAscii(columns));
    if (TracingCellScanner.log.isTraceEnabled()) {
      return new TracingCellScanner(wrappedBuilder.build(), txid, scanId);
    } else {
      return wrappedBuilder.build();
    }
  }

  @Override
  public RowScannerBuilder byRow() {
    String scanId = Integer.toHexString(Math.abs(Objects.hash(span, columns, txid)));
    return new RowScannerBuilder() {
      @Override
      public RowScanner build() {
        log.trace("txid: {} scanId: {} scanner().over({}).fetch({}).byRow().build()", txid, scanId,
            Hex.encNonAscii(span), Hex.encNonAscii(columns));
        if (TracingCellScanner.log.isTraceEnabled()) {
          return new TracingRowScanner(wrappedBuilder.byRow().build(), txid, scanId);
        } else {
          return wrappedBuilder.byRow().build();
        }
      }
    };
  }
}
