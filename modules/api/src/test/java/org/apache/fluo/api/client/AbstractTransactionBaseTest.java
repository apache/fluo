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

package org.apache.fluo.api.client;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.fluo.api.client.scanner.ScannerBuilder;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.exceptions.AlreadySetException;
import org.apache.fluo.api.exceptions.CommitException;
import org.junit.Assert;
import org.junit.Test;

public class AbstractTransactionBaseTest {

  private static class MockTransaction extends AbstractTransactionBase implements Transaction {

    public Set<RowColumnValue> sets = new HashSet<>();
    public Set<RowColumn> deletes = new HashSet<>();
    public Set<RowColumn> weakNtfys = new HashSet<>();
    public Map<RowColumn, Bytes> snapshot;

    MockTransaction(Map<RowColumn, Bytes> snapshot) {
      this.snapshot = snapshot;
    }

    @Override
    public void delete(Bytes row, Column col) {
      deletes.add(new RowColumn(row, col));
    }

    @Override
    public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
      sets.add(new RowColumnValue(row, col, value));
    }

    @Override
    public void setWeakNotification(Bytes row, Column col) {
      weakNtfys.add(new RowColumn(row, col));
    }

    @Override
    public Bytes get(Bytes row, Column column) {
      return snapshot.get(new RowColumn(row, column));
    }

    @Override
    public Map<Column, Bytes> get(Bytes row, Set<Column> columns) {
      HashMap<Column, Bytes> ret = new HashMap<Column, Bytes>();
      for (Column column : columns) {
        RowColumn rc = new RowColumn(row, column);
        if (snapshot.containsKey(rc)) {
          ret.put(column, snapshot.get(rc));
        }
      }
      return ret;
    }

    @Override
    public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns) {
      Map<Bytes, Map<Column, Bytes>> ret = new HashMap<>();

      for (Bytes row : rows) {
        for (Column col : columns) {
          RowColumn rc = new RowColumn(row, col);
          if (snapshot.containsKey(rc)) {
            ret.computeIfAbsent(row, k -> new HashMap<>()).put(col, snapshot.get(rc));
          }
        }
      }

      return ret;
    }

    @Override
    public Map<RowColumn, Bytes> get(Collection<RowColumn> rowColumns) {
      Map<RowColumn, Bytes> ret = new HashMap<>();
      for (RowColumn rc : rowColumns) {
        if (snapshot.containsKey(rc)) {
          ret.put(rc, snapshot.get(rc));
        }
      }
      return ret;
    }

    @Override
    public ScannerBuilder scanner() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getStartTimestamp() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void commit() throws CommitException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {}

  }

  private static final Column COL1 = new Column("f1", "q1");
  private static final Column COL2 = new Column("f1", "q2");

  private Map<RowColumn, Bytes> createSnapshot() {
    Map<RowColumn, Bytes> snap = new HashMap<>();
    snap.put(new RowColumn("row1", COL1), Bytes.of("v1"));
    snap.put(new RowColumn("row2", COL1), Bytes.of("v2"));
    snap.put(new RowColumn("row2", COL2), Bytes.of("v3"));
    snap.put(new RowColumn("row3", COL1), Bytes.of("v4"));
    return snap;
  }

  @Test
  public void testColumnsVarargs() {

    MockTransaction tx = new MockTransaction(createSnapshot());
    Assert
        .assertEquals(ImmutableMap.of(COL1, Bytes.of("v1")), tx.get(Bytes.of("row1"), COL1, COL2));
    Assert.assertEquals(ImmutableMap.of(COL1, Bytes.of("v2"), COL2, Bytes.of("v3")),
        tx.get(Bytes.of("row2"), COL1, COL2));

    Assert.assertEquals(ImmutableMap.of(COL1, "v1"), tx.gets("row1", COL1, COL2));
    Assert.assertEquals(ImmutableMap.of(COL1, "v2", COL2, "v3"), tx.gets("row2", COL1, COL2));

    Map<Bytes, Map<Column, Bytes>> ret =
        tx.get(Arrays.asList(Bytes.of("row1"), Bytes.of("row2")), COL1, COL2);
    Assert.assertEquals(ImmutableSet.of(Bytes.of("row1"), Bytes.of("row2")), ret.keySet());
    Assert.assertEquals(ImmutableMap.of(COL1, Bytes.of("v1")), ret.get(Bytes.of("row1")));
    Assert.assertEquals(ImmutableMap.of(COL1, Bytes.of("v2"), COL2, Bytes.of("v3")),
        ret.get(Bytes.of("row2")));

    Map<String, Map<Column, String>> ret2 = tx.gets(Arrays.asList("row1", "row2"), COL1, COL2);
    Assert.assertEquals(ImmutableSet.of("row1", "row2"), ret2.keySet());
    Assert.assertEquals(ImmutableMap.of(COL1, "v1"), ret2.get("row1"));
    Assert.assertEquals(ImmutableMap.of(COL1, "v2", COL2, "v3"), ret2.get("row2"));

    tx.close();
  }

  @Test
  public void testGetStringMethods() {
    MockTransaction tx = new MockTransaction(createSnapshot());

    Assert.assertNull(tx.gets("row4", COL1));
    Assert.assertEquals("v4", tx.gets("row3", COL1));

    RowColumn rc1 = new RowColumn("row1", COL1);
    RowColumn rc2 = new RowColumn("row2", COL2);

    Map<RowColumn, String> ret = tx.gets(Arrays.asList(rc1, rc2));
    Assert.assertEquals(ImmutableMap.of(rc1, "v1", rc2, "v3"), ret);

    tx.close();
  }

  @Test
  public void testMutationMethods() {
    MockTransaction tx = new MockTransaction(createSnapshot());

    tx.delete("row5", COL1);
    tx.set("row9", COL2, "99");
    tx.set("row8", COL2, "88");
    tx.setWeakNotification("row5", COL2);

    Assert.assertEquals(ImmutableSet.of(new RowColumn("row5", COL1)), tx.deletes);
    Assert.assertEquals(ImmutableSet.of(new RowColumnValue("row9", COL2, "99"), new RowColumnValue(
        "row8", COL2, "88")), tx.sets);
    Assert.assertEquals(ImmutableSet.of(new RowColumn("row5", COL2)), tx.weakNtfys);

    tx.close();
  }

  @Test
  public void testGetDefault() {
    MockTransaction tx = new MockTransaction(createSnapshot());

    Assert.assertEquals(Bytes.of("vd"), tx.get(Bytes.of("row4"), COL1, Bytes.of("vd")));
    Assert.assertEquals("vd", tx.gets("row4", COL1, "vd"));

    tx.close();
  }
}
