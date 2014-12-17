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
package io.fluo.core.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.values.DelLockValue;
import io.fluo.accumulo.values.WriteValue;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.TransactionImpl;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Utilities for modifying columns in Fluo
 */
public class ColumnUtil {
  
  private ColumnUtil() {}

  public static byte[] concatCFCQ(Column c) {
    return Bytes.concat(c.getFamily(), c.getQualifier()).toArray();
  }

  static ColumnVisibility gv(Environment env, Column col) {
    return env.getSharedResources().getVisCache().getCV(col);
  }

  public static void commitColumn(Environment env, boolean isTrigger, boolean isPrimary, Column col, boolean isWrite, boolean isDelete, long startTs,
      long commitTs,
      Set<Column> observedColumns, Mutation m) {
    if (isWrite) {
      Flutation.put(env, m, col, ColumnConstants.WRITE_PREFIX | commitTs, WriteValue.encode(startTs, isPrimary, false));
    } else {
      Flutation.put(env, m, col, ColumnConstants.DEL_LOCK_PREFIX | commitTs, DelLockValue.encode(startTs, isPrimary, false));
    }
    
    if (isTrigger) {
      Flutation.put(env, m, col, ColumnConstants.ACK_PREFIX | startTs, TransactionImpl.EMPTY);
      m.putDelete(ColumnConstants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(col), gv(env, col), startTs);
    }
    if (observedColumns.contains(col) && isWrite && !isDelete) {
      m.put(ColumnConstants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(col), gv(env, col), commitTs, TransactionImpl.EMPTY);
    }
  }
  
  public static Entry<Key,Value> checkColumn(Environment env, IteratorSetting iterConf, Bytes row, Column col) {
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
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    if (iter.hasNext()) {
      Entry<Key,Value> entry = iter.next();
      
      Key k = entry.getKey();
      Bytes r = Bytes.wrap(k.getRowData().toArray());
      Bytes cf = Bytes.wrap(k.getColumnFamilyData().toArray());
      Bytes cq = Bytes.wrap(k.getColumnQualifierData().toArray());
      Bytes cv = Bytes.wrap(k.getColumnVisibilityData().toArray());
      
      if (r.equals(row) && cf.equals(col.getFamily()) && cq.equals(col.getQualifier())
          && cv.equals(col.getVisibility())) {
        return entry;
      } else {
        throw new RuntimeException("unexpected key " + k + " " + row + " " + col);
      }
    }
    
    return null;
  }
  
  public static void writeColumn(Column col, DataOutput out) throws IOException {
    Bytes.write(out, col.getFamily());
    Bytes.write(out, col.getQualifier());
    Bytes.write(out, col.getVisibility());    
  }

  public static Column readColumn(DataInput in) throws IOException {
    Bytes family = Bytes.read(in);
    Bytes qualifier = Bytes.read(in);
    Bytes visibility = Bytes.read(in);
    return new Column(family, qualifier, visibility);
  }
}
