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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.values.DelLockValue;
import org.apache.fluo.accumulo.values.WriteValue;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.TransactionImpl;

/**
 * Utilities for modifying columns in Fluo
 */
public class ColumnUtil {

  private ColumnUtil() {}

  static ColumnVisibility gv(Environment env, Column col) {
    return env.getSharedResources().getVisCache().getCV(col);
  }

  public static void commitColumn(Environment env, boolean isTrigger, boolean isPrimary,
      Column col, boolean isWrite, boolean isDelete, long startTs, long commitTs,
      Set<Column> observedColumns, Mutation m) {
    if (isWrite) {
      Flutation.put(env, m, col, ColumnConstants.WRITE_PREFIX | commitTs,
          WriteValue.encode(startTs, isPrimary, isDelete));
    } else {
      Flutation.put(env, m, col, ColumnConstants.DEL_LOCK_PREFIX | startTs,
          DelLockValue.encodeCommit(commitTs, isPrimary));
    }

    if (isTrigger) {
      Flutation.put(env, m, col, ColumnConstants.ACK_PREFIX | startTs, TransactionImpl.EMPTY);
    }
  }

  public static Entry<Key, Value> checkColumn(Environment env, IteratorSetting iterConf, Bytes row,
      Column col) {
    Span span = Span.exact(row, col);

    Scanner scanner;
    try {
      // TODO reuse or share scanner
      scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
    } catch (TableNotFoundException e) {
      // TODO proper exception handling
      throw new RuntimeException(e);
    }
    scanner.setRange(SpanUtil.toRange(span));
    scanner.addScanIterator(iterConf);

    Iterator<Entry<Key, Value>> iter = scanner.iterator();
    if (iter.hasNext()) {
      Entry<Key, Value> entry = iter.next();

      Key k = entry.getKey();
      Bytes r = Bytes.of(k.getRowData().toArray());
      Bytes cf = Bytes.of(k.getColumnFamilyData().toArray());
      Bytes cq = Bytes.of(k.getColumnQualifierData().toArray());
      Bytes cv = Bytes.of(k.getColumnVisibilityData().toArray());

      if (r.equals(row) && cf.equals(col.getFamily()) && cq.equals(col.getQualifier())
          && cv.equals(col.getVisibility())) {
        return entry;
      } else {
        throw new RuntimeException("unexpected key " + k + " " + row + " " + col);
      }
    }

    return null;
  }

  public static void writeColumn(Column col, DataOutputStream out) throws IOException {
    ByteUtil.write(out, col.getFamily());
    ByteUtil.write(out, col.getQualifier());
    ByteUtil.write(out, col.getVisibility());
  }

  public static Column readColumn(DataInputStream in) throws IOException {
    BytesBuilder bb = Bytes.builder();
    Bytes family = ByteUtil.read(bb, in);
    Bytes qualifier = ByteUtil.read(bb, in);
    Bytes visibility = ByteUtil.read(bb, in);
    return new Column(family, qualifier, visibility);
  }

  public static Column convert(Key k) {
    Bytes f = ByteUtil.toBytes(k.getColumnFamilyData());
    Bytes q = ByteUtil.toBytes(k.getColumnQualifierData());
    Bytes v = ByteUtil.toBytes(k.getColumnVisibilityData());
    return new Column(f, q, v);
  }
}
