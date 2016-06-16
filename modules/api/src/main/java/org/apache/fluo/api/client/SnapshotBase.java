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

import org.apache.fluo.api.config.ScannerConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.iterator.RowIterator;

/**
 * Allows users to read from a Fluo table at a certain point in time
 */
public interface SnapshotBase {

  /**
   * Retrieves the value (in {@link Bytes}) stored at a given row and {@link Column}. Returns null
   * if does not exist.
   */
  Bytes get(Bytes row, Column column);

  /**
   * Given a row and set of {@link Column}s, retrieves a map contains the values at those
   * {@link Column}s. Only columns that exist will be returned in map.
   */
  Map<Column, Bytes> get(Bytes row, Set<Column> columns);

  /**
   * Given a collection of rows and set of {@link Column}s, retrieves a map contains the values at
   * those rows and {@link Column}s. Only rows and columns that exists will be returned in map.
   */
  Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns);

  /**
   * Given a collection of {@link RowColumn}s, retrieves a map contains the values at those rows and
   * {@link Column}s. Only rows and columns that exists will be returned in map.
   */
  Map<Bytes, Map<Column, Bytes>> get(Collection<RowColumn> rowColumns);

  /**
   * Retrieves a {@link RowIterator} with the given {@link ScannerConfiguration}
   */
  RowIterator get(ScannerConfiguration config);

  /**
   * Wrapper for {@link #get(Collection)} that uses Strings. All strings are encoded and decoded
   * using UTF-8.
   */
  Map<String, Map<Column, String>> gets(Collection<RowColumn> rowColumns);

  /**
   * Wrapper for {@link #get(Collection, Set)} that uses Strings. All strings are encoded and
   * decoded using UTF-8.
   */
  Map<String, Map<Column, String>> gets(Collection<String> rows, Set<Column> columns);

  /**
   * Wrapper for {@link #get(Bytes, Column)} that uses Strings. All strings are encoded and decoded
   * using UTF-8.
   */
  String gets(String row, Column column);

  /**
   * Wrapper for {@link #get(Bytes, Set)} that uses Strings. All strings are encoded and decoded
   * using UTF-8.
   */
  Map<Column, String> gets(String row, Set<Column> columns);

  /**
   * @return transactions start timestamp allocated from Oracle.
   */
  long getStartTimestamp();
}
