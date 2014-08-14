/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.api.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import org.apache.accumulo.core.util.ArgumentChecker;

/**
 * Configures Scanner
 */
public class ScannerConfiguration implements Cloneable {

  private Span span = new Span();
  // TODO use a set
  private ArrayList<Column> columns = new ArrayList<Column>();

  public ScannerConfiguration setSpan(Span span) {
    this.span = span;
    return this;
  }

  public Span getSpan() {
    return span;
  }

  public List<Column> getColumns() {
    return Collections.unmodifiableList(columns);
  }

  // TODO document SnapshotIterator

  public ScannerConfiguration fetchColumnFamily(Bytes col) {
    ArgumentChecker.notNull(col);
    // TODO causes NPE w/ set, add unit test
    columns.add(new Column(col, null));
    return this;
  }

  public ScannerConfiguration fetchColumn(Bytes colFam, Bytes colQual) {
    ArgumentChecker.notNull(colFam, colQual);
    columns.add(new Column(colFam, colQual));
    return this;
  }


  public void clearColumns() {
    columns.clear();
  }

  @SuppressWarnings("unchecked")
  public Object clone() throws CloneNotSupportedException {
    ScannerConfiguration sc = (ScannerConfiguration) super.clone();

    sc.columns = (ArrayList<Column>) columns.clone();
    sc.span = span;

    return sc;
  }
}
