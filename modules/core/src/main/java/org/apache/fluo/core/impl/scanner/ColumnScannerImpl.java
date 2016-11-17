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
import java.util.function.Function;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.core.util.ByteUtil;

public class ColumnScannerImpl implements ColumnScanner {

  public ColumnValue entry2cv(Entry<Key, Value> entry) {
    Column col = columnConverter.apply(entry.getKey());
    Bytes val = Bytes.of(entry.getValue().get());
    return new ColumnValue(col, val);
  }

  private PeekingIterator<Entry<Key, Value>> peekingIter;
  private Bytes row;
  private Iterator<ColumnValue> iter;
  private boolean gotIter = false;
  private Function<Key, Column> columnConverter;

  ColumnScannerImpl(Iterator<Entry<Key, Value>> e, Function<Key, Column> columnConverter) {
    peekingIter = Iterators.peekingIterator(e);
    this.columnConverter = columnConverter;
    row = ByteUtil.toBytes(peekingIter.peek().getKey().getRowData());
    iter = Iterators.transform(peekingIter, this::entry2cv);
  }

  @Override
  public Iterator<ColumnValue> iterator() {
    Preconditions.checkState(!gotIter,
        "Unfortunately this implementation only support getting the iterator once");
    gotIter = true;
    return iter;
  }

  @Override
  public Bytes getRow() {
    return row;
  }

  @Override
  public String getsRow() {
    return getRow().toString();
  }
}
