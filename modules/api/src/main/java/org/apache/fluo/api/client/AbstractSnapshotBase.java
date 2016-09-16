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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;

/**
 * This class provides default implementations for many of the classes in SnapshotBase. It exists to
 * make implementing SnapshotBase easier.
 */

public abstract class AbstractSnapshotBase implements SnapshotBase {

  public Bytes get(Bytes row, Column column, Bytes defaultValue) {
    Bytes ret = get(row, column);
    if (ret == null) {
      return defaultValue;
    }

    return ret;
  }

  public Map<Column, Bytes> get(Bytes row, Column... columns) {
    return get(row, ImmutableSet.copyOf(columns));
  }

  public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Column... columns) {
    return get(rows, ImmutableSet.copyOf(columns));
  }

  public Map<RowColumn, String> gets(Collection<RowColumn> rowColumns) {
    Map<RowColumn, Bytes> bytesMap = get(rowColumns);
    return Maps.transformValues(bytesMap, b -> b.toString());
  }

  public Map<String, Map<Column, String>> gets(Collection<? extends CharSequence> rows,
      Set<Column> columns) {
    Map<Bytes, Map<Column, Bytes>> rcvs =
        get(Collections2.transform(rows, s -> Bytes.of(s)), columns);
    Map<String, Map<Column, String>> ret = new HashMap<>(rcvs.size());

    for (Entry<Bytes, Map<Column, Bytes>> entry : rcvs.entrySet()) {
      ret.put(entry.getKey().toString(), Maps.transformValues(entry.getValue(), b -> b.toString()));
    }
    return ret;
  }

  public Map<String, Map<Column, String>> gets(Collection<? extends CharSequence> rows,
      Column... columns) {
    return gets(rows, ImmutableSet.copyOf(columns));
  }

  public String gets(CharSequence row, Column column) {
    Bytes val = get(Bytes.of(row), column);
    if (val == null) {
      return null;
    }
    return val.toString();
  }

  public String gets(CharSequence row, Column column, String defaultValue) {
    Bytes val = get(Bytes.of(row), column);
    if (val == null) {
      return defaultValue;
    }

    return val.toString();
  }

  public Map<Column, String> gets(CharSequence row, Set<Column> columns) {
    Map<Column, Bytes> values = get(Bytes.of(row), columns);
    return Maps.transformValues(values, b -> b.toString());
  }

  public Map<Column, String> gets(CharSequence row, Column... columns) {
    return gets(row, ImmutableSet.copyOf(columns));
  }
}
