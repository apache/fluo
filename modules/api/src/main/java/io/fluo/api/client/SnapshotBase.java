/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.api.client;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.iterator.RowIterator;

/**
 * Allows users to read from a Fluo table at a certain point in time
 */
public interface SnapshotBase {

  /**
   * Retrieves the value (in {@link Bytes}) stored at a given row and {@link Column}. Returns null
   * if does not exist.
   */
  public Bytes get(Bytes row, Column column);

  /**
   * Given a row and set of {@link Column}s, retrieves a map contains the values at those
   * {@link Column}s. Only columns that exist will be returned in map.
   */
  public Map<Column, Bytes> get(Bytes row, Set<Column> columns);

  /**
   * Given a collection of rows and set of {@link Column}s, retrieves a map contains the values at
   * those rows and {@link Column}s. Only rows and columns that exists will be returned in map.
   */
  public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns);

  /**
   * Retrieves a {@link RowIterator} with the given {@link ScannerConfiguration}
   */
  public RowIterator get(ScannerConfiguration config);
}
