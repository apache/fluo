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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;

public class TxStringUtil {
  private static Map<String, Map<Column, String>> transform(Map<Bytes, Map<Column, Bytes>> rcvs) {
    Map<String, Map<Column, String>> ret = new HashMap<>(rcvs.size());

    for (Entry<Bytes, Map<Column, Bytes>> entry : rcvs.entrySet()) {
      ret.put(entry.getKey().toString(), Maps.transformValues(entry.getValue(), b -> b.toString()));
    }
    return ret;
  }

  public static String gets(SnapshotBase snapshot, CharSequence row, Column column) {
    Bytes val = snapshot.get(Bytes.of(row), column);
    if (val == null) {
      return null;
    }
    return val.toString();
  }

  public static Map<Column, String> gets(SnapshotBase snapshot, CharSequence row,
      Set<Column> columns) {
    Map<Column, Bytes> values = snapshot.get(Bytes.of(row), columns);
    return Maps.transformValues(values, b -> b.toString());
  }

  public static Map<String, Map<Column, String>> gets(SnapshotBase snapshot,
      Collection<? extends CharSequence> rows, Set<Column> columns) {
    return transform(snapshot.get(Collections2.transform(rows, s -> Bytes.of(s)), columns));
  }

  public static Map<RowColumn, String> gets(SnapshotBase snapshot, Collection<RowColumn> rowColumns) {
    Map<RowColumn, Bytes> bytesMap = snapshot.get(rowColumns);
    return Maps.transformValues(bytesMap, b -> b.toString());
  }
}
