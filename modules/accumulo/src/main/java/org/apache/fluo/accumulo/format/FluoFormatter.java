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

package org.apache.fluo.accumulo.format;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.ColumnType;
import org.apache.fluo.accumulo.util.NotificationUtil;
import org.apache.fluo.accumulo.util.ReadLockUtil;
import org.apache.fluo.accumulo.values.DelLockValue;
import org.apache.fluo.accumulo.values.DelReadLockValue;
import org.apache.fluo.accumulo.values.LockValue;
import org.apache.fluo.accumulo.values.ReadLockValue;
import org.apache.fluo.accumulo.values.WriteValue;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

/**
 * Converts Accumulo table data to a human-readable Fluo format
 */
public class FluoFormatter {

  private static void appendByte(StringBuilder sb, byte b) {
    if (b >= 32 && b <= 126 && b != '\\') {
      sb.append((char) b);
    } else {
      sb.append(String.format("\\x%02x", b & 0xff));
    }
  }

  public static void encNonAscii(StringBuilder sb, byte[] bytes) {
    for (int i = 0; i < bytes.length; i++) {
      appendByte(sb, bytes[i]);
    }
  }

  public static void encNonAscii(StringBuilder sb, ByteSequence bytes) {
    for (int i = 0; i < bytes.length(); i++) {
      appendByte(sb, bytes.byteAt(i));
    }
  }

  public static void encNonAscii(StringBuilder sb, Bytes bytes) {
    for (int i = 0; i < bytes.length(); i++) {
      appendByte(sb, bytes.byteAt(i));
    }
  }

  public static String toString(Entry<Key, Value> entry) {
    Key key = entry.getKey();

    if (NotificationUtil.isNtfy(key)) {
      StringBuilder sb = new StringBuilder();
      encNonAscii(sb, key.getRowData());
      sb.append(" ");
      encNonAscii(sb, key.getColumnFamilyData());
      sb.append(":");
      Column col = NotificationUtil.decodeCol(key);
      encNonAscii(sb, col.getFamily());
      sb.append(":");
      encNonAscii(sb, col.getQualifier());
      sb.append(" [");
      encNonAscii(sb, key.getColumnVisibilityData());
      sb.append("] ");
      sb.append(NotificationUtil.decodeTs(key));
      sb.append('-');
      sb.append(NotificationUtil.isDelete(key) ? "DELETE" : "INSERT");
      sb.append("\t");
      encNonAscii(sb, entry.getValue().get());

      return sb.toString();
    } else {
      long ts = key.getTimestamp();
      String type;
      ColumnType colType = ColumnType.from(ts);

      switch (colType) {
        case RLOCK:
          if (ReadLockUtil.isDelete(ts)) {
            type = "DEL_RLOCK";
          } else {
            type = "RLOCK";
          }
          ts = ReadLockUtil.decodeTs(ts);
          break;
        default:
          type = colType.toString();
          break;
      }

      StringBuilder sb = new StringBuilder();

      String val;
      if (type.equals("WRITE")) {
        val = new WriteValue(entry.getValue().get()).toString();
      } else if (type.equals("DEL_LOCK")) {
        val = new DelLockValue(entry.getValue().get()).toString();
      } else if (type.equals("LOCK")) {
        // TODO can Value be made to extend Bytes w/o breaking API?
        val = new LockValue(entry.getValue().get()).toString();
      } else if (type.equals("RLOCK")) {
        val = new ReadLockValue(entry.getValue().get()).toString();
      } else if (type.equals("DEL_RLOCK")) {
        val = new DelReadLockValue(entry.getValue().get()).toString();
      } else {
        encNonAscii(sb, entry.getValue().get());
        val = sb.toString();
      }

      sb.setLength(0);
      encNonAscii(sb, key.getRowData());
      sb.append(" ");
      encNonAscii(sb, key.getColumnFamilyData());
      sb.append(":");
      encNonAscii(sb, key.getColumnQualifierData());
      sb.append(" [");
      encNonAscii(sb, key.getColumnVisibilityData());
      sb.append("] ");
      sb.append(ts & ColumnConstants.TIMESTAMP_MASK);
      sb.append('-');
      sb.append(type);
      sb.append("\t");
      sb.append(val);

      return sb.toString();
    }
  }
}
