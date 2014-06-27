/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fluo.impl;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import org.fluo.api.Column;


/**
 * 
 */
public class ColumnUtil {
  public static final long PREFIX_MASK = 0xe000000000000000l;
  public static final long TX_DONE_PREFIX = 0x6000000000000000l;
  public static final long WRITE_PREFIX = 0x4000000000000000l;
  public static final long DEL_LOCK_PREFIX = 0x2000000000000000l;
  public static final long LOCK_PREFIX = 0xe000000000000000l;
  public static final long ACK_PREFIX = 0xc000000000000000l;
  public static final long DATA_PREFIX = 0xa000000000000000l;
  
  public static final long TIMESTAMP_MASK = 0x1fffffffffffffffl;


  public static byte[] concatCFCQ(Column c) {
    return ByteUtil.concat(c.getFamily(), c.getQualifier());
  }

  public static void commitColumn(boolean isTrigger, boolean isPrimary, Column col, boolean isWrite, boolean isDelete, long startTs, long commitTs,
      Set<Column> observedColumns, Mutation m) {
    if (isWrite) {
      m.put(col.getFamily().toArray(), col.getQualifier().toArray(), col.getVisibility(), WRITE_PREFIX | commitTs, WriteValue.encode(startTs, isPrimary, false));
    } else {
      m.put(col.getFamily().toArray(), col.getQualifier().toArray(), col.getVisibility(), DEL_LOCK_PREFIX | commitTs,
          DelLockValue.encode(startTs, isPrimary, false));
    }
    
    if (isTrigger) {
      m.put(col.getFamily().toArray(), col.getQualifier().toArray(), col.getVisibility(), ACK_PREFIX | startTs, TransactionImpl.EMPTY);
      m.putDelete(Constants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(col), col.getVisibility(), startTs);
    }
    if (observedColumns.contains(col) && isWrite && !isDelete) {
      m.put(Constants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(col), col.getVisibility(), commitTs, TransactionImpl.EMPTY);
    }
  }
  
  public static Entry<Key,Value> checkColumn(Configuration config, IteratorSetting iterConf, ByteSequence row, Column col) {
    Range range = Range.exact(ByteUtil.toText(row), ByteUtil.toText(col.getFamily()), ByteUtil.toText(col.getQualifier()), new Text(col.getVisibility()
        .getExpression()));
    
    Scanner scanner;
    try {
      // TODO reuse or share scanner
      scanner = config.getConnector().createScanner(config.getTable(), config.getAuthorizations());
    } catch (TableNotFoundException e) {
      // TODO proper exception handling
      throw new RuntimeException(e);
    }
    scanner.setRange(range);
    scanner.addScanIterator(iterConf);
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    if (iter.hasNext()) {
      Entry<Key,Value> entry = iter.next();
      
      Key k = entry.getKey();
      
      if (k.getRowData().equals(row) && k.getColumnFamilyData().equals(col.getFamily()) && k.getColumnQualifierData().equals(col.getQualifier())
          && k.getColumnVisibilityData().equals(new ArrayByteSequence(col.getVisibility().getExpression()))) {
        return entry;
      } else {
        throw new RuntimeException("unexpected key " + k + " " + row + " " + col);
      }
    }
    
    return null;
  }
}
