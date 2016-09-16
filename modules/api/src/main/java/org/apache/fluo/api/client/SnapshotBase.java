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

package org.apache.fluo.api.client;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.fluo.api.client.scanner.ScannerBuilder;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;

/**
 * Allows users to read from a Fluo table at a certain point in time
 *
 * @since 1.0.0
 * @see AbstractSnapshotBase
 */
public interface SnapshotBase {

  /**
   * Retrieves the value (in {@link Bytes}) stored at a given row and {@link Column}. Returns null
   * if does not exist.
   */
  Bytes get(Bytes row, Column column);

  /**
   * Retrieves the value (in {@link Bytes}) stored at a given row and {@link Column}. Returns the
   * passed in defaultValue if does not exist.
   * 
   * @param defaultValue this will be returned if row+columns does not exists
   */
  Bytes get(Bytes row, Column column, Bytes defaultValue);

  /**
   * Given a row and set of {@link Column}s, retrieves a map that contains the values at those
   * {@link Column}s. Only columns that exist will be returned in map.
   */
  Map<Column, Bytes> get(Bytes row, Set<Column> columns);

  /**
   * Given a row and list of {@link Column}s, retrieves a map that contains the values at those
   * {@link Column}s. Only columns that exist will be returned in map.
   */
  Map<Column, Bytes> get(Bytes row, Column... columns);

  /**
   * Given a collection of rows and set of {@link Column}s, retrieves a map that contains the values
   * at those rows and {@link Column}s. Only rows and columns that exists will be returned in map.
   */
  Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns);

  /**
   * Given a collection of rows and list of {@link Column}s, retrieves a map that contains the
   * values at those rows and {@link Column}s. Only rows and columns that exists will be returned in
   * map.
   */
  Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Column... columns);


  /**
   * Given a collection of {@link RowColumn}s, retrieves a map contains the values at
   * {@link RowColumn}. Only rows and columns that exists will be returned in map.
   */
  Map<RowColumn, Bytes> get(Collection<RowColumn> rowColumns);

  /**
   * This method is the starting point for constructing a scanner. Scanners can be constructed over
   * a {@link Span} and/or with a subset of columns. Below is simple example of building a scanner.
   * 
   * <pre>
   * <code>
   *   Transaction tx = ...;
   *   Span span = Span.exact("row4");
   *   Column col1 = new Column("fam1","qual1");
   *   Column col2 = new Column("fam1","qual2");
   * 
   *   //create a scanner over row4 fetching columns fam1:qual1 and fam1:qual2
   *   CellScanner cs = tx.scanner().over(span).fetch(col1,col2).build();
   *   for(RowColumnValue rcv : cs) {
   *     //do stuff with rcv
   *   }
   * </code>
   * </pre>
   *
   * <p>
   * The following example shows how to build a row scanner.
   *
   * <pre>
   * <code>
   *   RowScanner rs = tx.scanner().over(span).fetch(col1, col2).byRow().build();
   *   for (ColumnScanner colScanner : rs) {
   *     Bytes row = colScanner.getRow();
   *     for (ColumnValue cv : colScanner) {
   *       // do stuff with the columns and values in the row
   *     }
   *   }
   * </code>
   * </pre>
   *
   * @return A scanner builder.
   */

  ScannerBuilder scanner();

  /**
   * Wrapper for {@link #get(Collection)} that uses Strings. All strings are encoded and decoded
   * using UTF-8.
   */
  Map<RowColumn, String> gets(Collection<RowColumn> rowColumns);

  /**
   * Wrapper for {@link #get(Collection, Set)} that uses Strings. All strings are encoded and
   * decoded using UTF-8.
   */
  Map<String, Map<Column, String>> gets(Collection<? extends CharSequence> rows, Set<Column> columns);

  /**
   * Wrapper for {@link #get(Collection, Set)} that uses Strings. All strings are encoded and
   * decoded using UTF-8.
   */
  Map<String, Map<Column, String>> gets(Collection<? extends CharSequence> rows, Column... columns);

  /**
   * Wrapper for {@link #get(Bytes, Column)} that uses Strings. All strings are encoded and decoded
   * using UTF-8.
   */
  String gets(CharSequence row, Column column);

  /**
   * Wrapper for {@link #get(Bytes, Column, Bytes)} that uses Strings. All strings are encoded and
   * decoded using UTF-8.
   */
  String gets(CharSequence row, Column column, String defaultValue);

  /**
   * Wrapper for {@link #get(Bytes, Set)} that uses Strings. All strings are encoded and decoded
   * using UTF-8.
   */
  Map<Column, String> gets(CharSequence row, Set<Column> columns);

  /**
   * Wrapper for {@link #get(Bytes, Set)} that uses Strings. All strings are encoded and decoded
   * using UTF-8.
   */
  Map<Column, String> gets(CharSequence row, Column... columns);

  /**
   * @return transactions start timestamp allocated from Oracle.
   */
  long getStartTimestamp();
}
