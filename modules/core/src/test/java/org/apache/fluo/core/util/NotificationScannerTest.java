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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.NotificationUtil;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.junit.Assert;
import org.junit.Test;

public class NotificationScannerTest {

  private static class Data implements Iterable<Entry<Key, Value>> {
    TreeMap<Key, Value> data = new TreeMap<>();

    void putNtfy(String row, String fam, String qual) {
      byte[] r = row.getBytes(StandardCharsets.UTF_8);
      byte[] f = ColumnConstants.NOTIFY_CF.toArray();
      byte[] q = NotificationUtil.encodeCol(new Column(fam, qual));

      data.put(new Key(r, f, q, new byte[0], 42L), new Value(new byte[0]));
    }

    @Override
    public Iterator<Entry<Key, Value>> iterator() {
      return data.entrySet().iterator();
    }
  }


  /**
   * When scanning notifications, column filtering is done on the client side. This test ensures
   * that filtering works correctly.
   */
  @Test
  public void testColumnFiltering() {

    Data data = new Data();
    data.putNtfy("r001", "f8", "q2");
    data.putNtfy("r001", "f9", "q1");
    data.putNtfy("r002", "f8", "q2");
    data.putNtfy("r002", "f8", "q3");
    data.putNtfy("r004", "f9", "q3");
    data.putNtfy("r004", "f9", "q4");

    HashSet<RowColumnValue> expected = new HashSet<>();
    expected.add(new RowColumnValue("r001", new Column("f8", "q2"), ""));
    expected.add(new RowColumnValue("r001", new Column("f9", "q1"), ""));
    expected.add(new RowColumnValue("r002", new Column("f8", "q2"), ""));
    expected.add(new RowColumnValue("r002", new Column("f8", "q3"), ""));
    expected.add(new RowColumnValue("r004", new Column("f9", "q3"), ""));
    expected.add(new RowColumnValue("r004", new Column("f9", "q4"), ""));

    NotificationScanner scanner = new NotificationScanner(data, Collections.emptySet());
    HashSet<RowColumnValue> actual = new HashSet<>();
    scanner.forEach(actual::add);
    Assert.assertEquals(expected, actual);

    scanner = new NotificationScanner(data, Arrays.asList(new Column("f9")));
    actual.clear();
    scanner.forEach(actual::add);
    HashSet<RowColumnValue> expected2 = new HashSet<>();
    expected.stream().filter(rcv -> rcv.getColumn().getsFamily().equals("f9"))
        .forEach(expected2::add);
    Assert.assertEquals(expected2, actual);

    scanner = new NotificationScanner(data, Arrays.asList(new Column("f9"), new Column("f8")));
    actual.clear();
    scanner.forEach(actual::add);
    Assert.assertEquals(expected, actual);

    scanner = new NotificationScanner(data, Arrays.asList(new Column("f9", "q1")));
    actual.clear();
    scanner.forEach(actual::add);
    expected2.clear();
    expected2.add(new RowColumnValue("r001", new Column("f9", "q1"), ""));
    Assert.assertEquals(expected2, actual);

    scanner =
        new NotificationScanner(data, Arrays.asList(new Column("f9", "q1"), new Column("f8")));
    actual.clear();
    scanner.forEach(actual::add);
    expected2.clear();
    expected2.add(new RowColumnValue("r001", new Column("f9", "q1"), ""));
    expected2.add(new RowColumnValue("r001", new Column("f8", "q2"), ""));
    expected2.add(new RowColumnValue("r002", new Column("f8", "q2"), ""));
    expected2.add(new RowColumnValue("r002", new Column("f8", "q3"), ""));
    Assert.assertEquals(expected2, actual);
  }
}
