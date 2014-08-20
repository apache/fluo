/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * Implementations of this interface should provide snapshot isolation across rows when reading from a table.
 */
public interface Snapshot {

  public abstract Bytes get(Bytes row, Column column) throws Exception;

  public abstract Map<Column,Bytes> get(Bytes row, Set<Column> columns) throws Exception;

  public abstract Map<Bytes,Map<Column,Bytes>> get(Collection<Bytes> rows, Set<Column> columns) throws Exception;

  public abstract RowIterator get(ScannerConfiguration config) throws Exception;
}
