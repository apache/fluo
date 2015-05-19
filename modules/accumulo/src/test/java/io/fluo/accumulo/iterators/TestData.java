/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.accumulo.iterators;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import io.fluo.accumulo.format.FluoFormatter;
import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.values.DelLockValue;
import io.fluo.accumulo.values.LockValue;
import io.fluo.accumulo.values.WriteValue;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class TestData {
  TreeMap<Key, Value> data = new TreeMap<Key, Value>();

  TestData() {}

  TestData(TestData td) {
    data.putAll(td.data);
  }

  TestData(SortedKeyValueIterator<Key, Value> iter, Range range) {
    try {
      iter.seek(range, new HashSet<ByteSequence>(), false);

      while (iter.hasTop()) {
        data.put(iter.getTopKey(), iter.getTopValue());
        iter.next();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
    byte[] val = new byte[0];;

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
        ts |= ColumnConstants.ACK_PREFIX;
        break;
      case "TX_DONE":
        ts |= ColumnConstants.TX_DONE_PREFIX;
        break;
      case "WRITE":
        ts |= ColumnConstants.WRITE_PREFIX;
        long writeTs = Long.parseLong(value.split("\\s+")[0]);
        val = WriteValue.encode(writeTs, value.contains("PRIMARY"), value.contains("TRUNCATION"));
        break;
      case "LOCK":
        ts |= ColumnConstants.LOCK_PREFIX;
        String rc[] = value.split("\\s+");
        val =
            LockValue.encode(Bytes.of(rc[0]), new Column(rc[1], rc[2]), value.contains("WRITE"),
                value.contains("DELETE"), value.contains("TRIGGER"), 42l);
        break;
      case "DATA":
        ts |= ColumnConstants.DATA_PREFIX;
        val = value.getBytes();
        break;
      case "DEL_LOCK":
        ts |= ColumnConstants.DEL_LOCK_PREFIX;
        long lockTs = Long.parseLong(value.split("\\s+")[0]);
        val = DelLockValue.encode(lockTs, value.contains("PRIMARY"), value.contains("ROLLBACK"));
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
}
