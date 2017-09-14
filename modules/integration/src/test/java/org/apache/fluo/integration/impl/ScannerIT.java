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

package org.apache.fluo.integration.impl;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.client.scanner.RowScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.integration.ITBaseImpl;
import org.junit.Assert;
import org.junit.Test;

public class ScannerIT extends ITBaseImpl {

  @Test
  public void testFiltering() {
    Set<RowColumnValue> expected = genData();

    HashSet<RowColumnValue> expectedR2 = new HashSet<>();
    Iterables.addAll(expectedR2, Iterables.filter(expected, rcv -> rcv.getsRow().equals("r2")));
    Assert.assertEquals(2, expectedR2.size());

    HashSet<RowColumnValue> expectedR2c = new HashSet<>();
    Iterables.addAll(expectedR2c, Iterables.filter(expected,
        rcv -> rcv.getsRow().equals("r2") && rcv.getColumn().equals(new Column("f1", "q2"))));
    Assert.assertEquals(1, expectedR2c.size());

    HashSet<RowColumnValue> expectedC = new HashSet<>();
    Iterables.addAll(expectedC,
        Iterables.filter(expected, rcv -> rcv.getColumn().equals(new Column("f1", "q1"))));
    Assert.assertEquals(2, expectedC.size());

    HashSet<RowColumnValue> expectedCF = new HashSet<>();
    Iterables.addAll(expectedCF,
        Iterables.filter(expected, rcv -> rcv.getColumn().getsFamily().equals("f2")));
    Assert.assertEquals(2, expectedCF.size());

    HashSet<RowColumnValue> expectedCols = new HashSet<>();
    Iterables.addAll(expectedCols,
        Iterables.filter(expected, rcv -> rcv.getColumn().equals(new Column("f2", "q5"))
            || rcv.getColumn().equals(new Column("f1", "q1"))));
    Assert.assertEquals(3, expectedCols.size());

    try (Snapshot snap = client.newSnapshot()) {
      HashSet<RowColumnValue> actual = new HashSet<>();
      Iterables.addAll(actual, snap.scanner().over(Span.exact("r2")).build());
      Assert.assertEquals(expectedR2, actual);

      actual.clear();
      Iterables.addAll(actual,
          snap.scanner().over(Span.exact("r2")).fetch(new Column("f1", "q2")).build());
      Assert.assertEquals(expectedR2c, actual);

      actual.clear();
      Iterables.addAll(actual, snap.scanner().fetch(new Column("f1", "q1")).build());
      Assert.assertEquals(expectedC, actual);

      actual.clear();
      Iterables.addAll(actual, snap.scanner().fetch(new Column("f2")).build());
      Assert.assertEquals(expectedCF, actual);

      actual.clear();
      Iterables.addAll(actual,
          snap.scanner().fetch(new Column("f2", "q5"), new Column("f1", "q1")).build());
      Assert.assertEquals(expectedCols, actual);
    }

  }

  @Test
  public void testSame() {
    Set<RowColumnValue> expected = genData();

    Column col1 = new Column("f1", "q1");
    Column col2 = new Column("f2", "q3");

    HashSet<RowColumnValue> expectedC = new HashSet<>();
    Iterables.addAll(expectedC, Iterables.filter(expected,
        rcv -> rcv.getColumn().equals(col1) || rcv.getColumn().equals(col2)));
    Assert.assertEquals(3, expectedC.size());

    try (Snapshot snap = client.newSnapshot()) {
      CellScanner scanner = snap.scanner().fetch(col1, col2).build();

      HashSet<RowColumnValue> actual = new HashSet<>();
      Bytes prevRow = null;
      for (RowColumnValue rcv : scanner) {
        actual.add(rcv);

        Column c = rcv.getColumn();

        Assert.assertTrue((col1.equals(c) && col1 == c) || (col2.equals(c) && col2 == c));

        if (col2.equals(c)) {
          Assert.assertEquals(Bytes.of("r1"), rcv.getRow());
          Assert.assertSame(rcv.getRow(), prevRow);
        }

        prevRow = rcv.getRow();
      }

      Assert.assertEquals(expectedC, actual);
    }
  }

  @Test
  public void testMultipleIteratorsFromSameRowScanner() {
    Set<RowColumnValue> expected = genData();

    try (Snapshot snap = client.newSnapshot()) {
      RowScanner rowScanner = snap.scanner().byRow().build();

      Iterator<ColumnScanner> iter1 = rowScanner.iterator();
      Iterator<ColumnScanner> iter2 = rowScanner.iterator();

      HashSet<RowColumnValue> actual1 = new HashSet<>();
      HashSet<RowColumnValue> actual2 = new HashSet<>();

      while (iter1.hasNext()) {
        ColumnScanner cs1 = iter1.next();

        Assert.assertTrue(iter2.hasNext());
        ColumnScanner cs2 = iter2.next();

        for (ColumnValue cv : cs1) {
          actual1.add(new RowColumnValue(cs1.getRow(), cv.getColumn(), cv.getValue()));
        }

        for (ColumnValue cv : cs2) {
          actual2.add(new RowColumnValue(cs2.getRow(), cv.getColumn(), cv.getValue()));
        }
      }

      Assert.assertFalse(iter2.hasNext());

      Assert.assertEquals(expected, actual1);
      Assert.assertEquals(expected, actual2);
    }
  }

  @Test
  public void testMultipleIteratorsFromSameIterable() {

    Set<RowColumnValue> expected = genData();

    try (Snapshot snap = client.newSnapshot()) {
      CellScanner cellScanner = snap.scanner().build();
      // grab two iterators from same iterable and iterator over them in interleaved fashion
      Iterator<RowColumnValue> iter1 = cellScanner.iterator();
      Iterator<RowColumnValue> iter2 = cellScanner.iterator();

      HashSet<RowColumnValue> actual1 = new HashSet<>();
      HashSet<RowColumnValue> actual2 = new HashSet<>();

      while (iter1.hasNext()) {
        Assert.assertTrue(iter2.hasNext());
        actual1.add(iter1.next());
        actual2.add(iter2.next());
      }

      Assert.assertFalse(iter2.hasNext());

      Assert.assertEquals(expected, actual1);
      Assert.assertEquals(expected, actual2);
    }
  }

  private Set<RowColumnValue> genData() {
    Set<RowColumnValue> expected = new HashSet<>();
    expected.add(new RowColumnValue("r1", new Column("f1", "q1"), "v1"));
    expected.add(new RowColumnValue("r1", new Column("f2", "q3"), "v2"));
    expected.add(new RowColumnValue("r2", new Column("f1", "q1"), "v3"));
    expected.add(new RowColumnValue("r2", new Column("f1", "q2"), "v4"));
    expected.add(new RowColumnValue("r4", new Column("f2", "q5"), "v5"));

    Assert.assertEquals(5, expected.size());

    try (Transaction tx = client.newTransaction()) {
      for (RowColumnValue rcv : expected) {
        tx.set(rcv.getRow(), rcv.getColumn(), rcv.getValue());
      }
      tx.commit();
    }

    return expected;
  }
}
