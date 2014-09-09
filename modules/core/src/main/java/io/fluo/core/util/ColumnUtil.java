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

/**
 * Utilities for modifying columns in Fluo
 */
public class ColumnUtil {

  private ColumnUtil() {}

  public static byte[] concatCFCQ(Column c) {
    return Bytes.concat(c.getFamily(), c.getQualifier()).toArray();
  }

  public static void commitColumn(boolean isTrigger, boolean isPrimary, Column col, boolean isWrite, boolean isDelete, long startTs, long commitTs,
      Set<Column> observedColumns, Mutation m) {
    if (isWrite) {
      m.put(ByteUtil.toText(col.getFamily()), ByteUtil.toText(col.getQualifier()), col.getVisibilityParsed(), ColumnConstants.WRITE_PREFIX | commitTs, new Value(WriteValue.encode(startTs, isPrimary, false)));
    } else {
      m.put(ByteUtil.toText(col.getFamily()), ByteUtil.toText(col.getQualifier()), col.getVisibilityParsed(), ColumnConstants.DEL_LOCK_PREFIX | commitTs,
          new Value(DelLockValue.encode(startTs, isPrimary, false)));
    }

    if (isTrigger) {
      m.put(ByteUtil.toText(col.getFamily()), ByteUtil.toText(col.getQualifier()), col.getVisibilityParsed(), ColumnConstants.ACK_PREFIX | startTs, new Value(TransactionImpl.EMPTY));
      m.putDelete(ColumnConstants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(col), col.getVisibilityParsed(), startTs);
    }
    if (observedColumns.contains(col) && isWrite && !isDelete) {
      m.put(ColumnConstants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(col), col.getVisibilityParsed(), commitTs, TransactionImpl.EMPTY);
    }
  }

  public static Entry<Key,Value> checkColumn(Environment env, IteratorSetting iterConf, Bytes row, Column col) {
    Span span = Span.exact(row, col.getFamily(), col.getQualifier(), col.getVisibility());

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
}
