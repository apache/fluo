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

package org.apache.fluo.core.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

public class CachedColumnConverter implements Function<Key, Column> {
  private Map<ColumnKey, Column> colCache = new HashMap<>();
  private ColumnKey resuableKey = new ColumnKey();

  private static class ColumnKey {
    private ByteSequence family;
    private ByteSequence qualifier;
    private ByteSequence vis;
    private int hashCode = 0;

    ColumnKey() {}

    ColumnKey(Column col) {
      this.family = ByteUtil.toByteSequence(col.getFamily());
      this.qualifier = ByteUtil.toByteSequence(col.getQualifier());
      this.vis = ByteUtil.toByteSequence(col.getVisibility());
      this.hashCode = 0;
    }

    void reset(ByteSequence family, ByteSequence qualifier, ByteSequence vis) {
      this.family = family;
      this.qualifier = qualifier;
      this.vis = vis;
      this.hashCode = 0;
    }

    @Override
    public int hashCode() {
      if (hashCode == 0) {
        hashCode = family.hashCode();
        hashCode = 31 * hashCode + qualifier.hashCode();
        hashCode = 31 * hashCode + vis.hashCode();
      }

      return hashCode;
    }

    public boolean equals(Object o) {
      if (o instanceof ColumnKey) {
        ColumnKey ock = (ColumnKey) o;
        return family.equals(ock.family) && qualifier.equals(ock.qualifier) && vis.equals(ock.vis);
      }

      return false;
    }
  }

  public CachedColumnConverter(Collection<Column> cols) {
    for (Column col : cols) {
      colCache.put(new ColumnKey(col), col);
    }
  }

  @Override
  public Column apply(Key k) {
    return toColumn(k.getColumnFamilyData(), k.getColumnQualifierData(),
        k.getColumnVisibilityData());
  }

  private Column toColumn(ByteSequence family, ByteSequence qualifier, ByteSequence vis) {
    resuableKey.reset(family, qualifier, vis);
    Column col = colCache.get(resuableKey);

    if (col == null) {
      Bytes f = ByteUtil.toBytes(family);
      Bytes q = ByteUtil.toBytes(qualifier);
      Bytes v = ByteUtil.toBytes(vis);
      return new Column(f, q, v);
    }

    return col;
  }
}
