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

package org.apache.fluo.accumulo.iterators;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.fluo.accumulo.format.FluoFormatter;
import org.apache.fluo.accumulo.iterators.CountingIterator.Counter;
import org.apache.fluo.accumulo.util.ColumnType;
import org.apache.fluo.accumulo.util.ReadLockUtil;
import org.apache.fluo.accumulo.values.DelLockValue;
import org.apache.fluo.accumulo.values.DelReadLockValue;
import org.apache.fluo.accumulo.values.LockValue;
import org.apache.fluo.accumulo.values.WriteValue;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

public class TestData {
  TreeMap<Key, Value> data = new TreeMap<>();
  Counter counter = new Counter();

  TestData() {}

  TestData(TestData td) {
    data.putAll(td.data);
  }

  TestData(SortedKeyValueIterator<Key, Value> iter, Range range, boolean reseek) {
    try {
      iter.seek(range, new HashSet<ByteSequence>(), false);

      while (iter.hasTop()) {
        data.put(iter.getTopKey(), iter.getTopValue());
        if (reseek) {
          iter.seek(
              new Range(iter.getTopKey(), false, range.getEndKey(), range.isEndKeyInclusive()),
              new HashSet<ByteSequence>(), false);
        } else {
          iter.next();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  TestData(SortedKeyValueIterator<Key, Value> iter, Range range) {
    this(iter, range, false);
  }

  TestData(SortedKeyValueIterator<Key, Value> iter) {
    this(iter, new Range());
  }

  public TestData addIfInRange(String key, String value, Range range) {
    String[] fields = key.split("\\s+");

    String row = fields[0];
    String cf = fields[1];
    String cq = fields[2];
    String ct;
    long ts;
    byte[] val = new byte[0];

    if (cf.equals("ntfy")) {
      ts = Long.parseLong(fields[3]) << 1;
      ct = cf;
      if (fields.length == 5) {
        if (!fields[4].equals("DEL"))
          throw new IllegalArgumentException("bad ntfy");
        // its a delete
        ts |= 1l;
      }
    } else {
      ct = fields[3];
      ts = Long.parseLong(fields[4]);
    }

    switch (ct) {
      case "ACK":
        ts = ColumnType.ACK.enode(ts);
        break;
      case "TX_DONE":
        ts = ColumnType.TX_DONE.enode(ts);
        break;
      case "WRITE":
        ts = ColumnType.WRITE.enode(ts);
        long writeTs = Long.parseLong(value.split("\\s+")[0]);
        val = WriteValue.encode(writeTs, value.contains("PRIMARY"), value.contains("DELETE"));
        break;
      case "LOCK":
        ts = ColumnType.LOCK.enode(ts);
        String rc[] = value.split("\\s+");
        val = LockValue.encode(Bytes.of(rc[0]), new Column(rc[1], rc[2]), value.contains("WRITE"),
            value.contains("DELETE"), value.contains("TRIGGER"), 42l);
        break;
      case "DATA":
        ts = ColumnType.DATA.enode(ts);
        val = value.getBytes();
        break;
      case "DEL_LOCK":
        ts = ColumnType.DEL_LOCK.enode(ts);
        if (value.contains("ROLLBACK") || value.contains("ABORT")) {
          val = DelLockValue.encodeRollback(value.contains("PRIMARY"), true);
        } else {
          long commitTs = Long.parseLong(value.split("\\s+")[0]);
          val = DelLockValue.encodeCommit(commitTs, value.contains("PRIMARY"));
        }
        break;
      case "RLOCK":
        ts = ReadLockUtil.encodeTs(ts, false);
        ts = ColumnType.RLOCK.enode(ts);
        break;
      case "DEL_RLOCK":
        ts = ReadLockUtil.encodeTs(ts, true);
        ts = ColumnType.RLOCK.enode(ts);

        if (value.contains("ROLLBACK") || value.contains("ABORT")) {
          val = DelReadLockValue.encodeRollback();
        } else {
          long commitTs = Long.parseLong(value.split("\\s+")[0]);
          val = DelReadLockValue.encodeCommit(commitTs);
        }
        break;
      case "ntfy":
        break;
      default:
        throw new IllegalArgumentException("unknown column type " + ct);

    }

    Key akey = new Key(row, cf, cq, ts);
    if (range.contains(akey)) {
      data.put(akey, new Value(val));
    }

    return this;
  }

  public TestData add(String key, String value) {
    return addIfInRange(key, value, new Range());
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TestData) {
      TestData otd = (TestData) o;
      return data.equals(otd.data);
    }
    return false;
  }

  @Override
  public String toString() {
    Set<Entry<Key, Value>> es = data.entrySet();
    StringBuilder sb = new StringBuilder("{");
    String sep = "";
    for (Entry<Key, Value> entry : es) {
      sb.append(sep);
      sep = ", ";
      sb.append(FluoFormatter.toString(entry));
    }

    sb.append("}");
    return sb.toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(data);
  }

  public SortedKeyValueIterator<Key, Value> getIterator() {
    return new CountingIterator(counter, data);
  }
}
