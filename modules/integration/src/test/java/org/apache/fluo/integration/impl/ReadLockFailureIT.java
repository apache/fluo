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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.accumulo.format.FluoFormatter;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.impl.TransactorNode;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.fluo.integration.impl.ReadLockIT.addEdge;
import static org.apache.fluo.integration.impl.ReadLockIT.setAlias;

public class ReadLockFailureIT extends ITBaseImpl {

  private void dumpTable(Consumer<String> out) throws TableNotFoundException {
    Scanner scanner = conn.createScanner(getCurTableName(), Authorizations.EMPTY);
    for (Entry<Key, Value> entry : scanner) {
      out.accept(FluoFormatter.toString(entry));
    }
  }

  private Set<String> getDerivedEdges() {
    Set<String> derivedEdges = new HashSet<>();
    try (Snapshot snap = client.newSnapshot()) {
      snap.scanner().over(Span.prefix("d:")).build().stream().map(RowColumnValue::getsRow)
          .map(r -> r.substring(2)).forEach(derivedEdges::add);
    }
    return derivedEdges;
  }

  private void expectCommitException(Consumer<Transaction> retryAction) {
    try (Transaction tx = client.newTransaction()) {
      retryAction.accept(tx);
      tx.commit();
      Assert.fail();
    } catch (CommitException ce) {

    }
  }

  private void retryOnce(Consumer<Transaction> retryAction) {

    expectCommitException(retryAction);

    try (Transaction tx = client.newTransaction()) {
      retryAction.accept(tx);
      tx.commit();
    }
  }

  private void retryTwice(Consumer<Transaction> retryAction) {

    expectCommitException(retryAction);
    expectCommitException(retryAction);

    try (Transaction tx = client.newTransaction()) {
      retryAction.accept(tx);
      tx.commit();
    }
  }


  private TransactorNode partiallyCommit(Consumer<TransactionBase> action, boolean commitPrimary,
      boolean closeTransactor) throws Exception {
    TransactorNode t2 = new TransactorNode(env);
    TestTransaction tx2 = new TestTransaction(env, t2);

    action.accept(tx2);

    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));

    if (commitPrimary) {
      Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
      Assert.assertTrue(tx2.commitPrimaryColumn(cd, commitTs));
    }

    if (closeTransactor) {
      t2.close();
    }

    return t2;
  }

  private void testBasicRollback(boolean closeTransactor) throws Exception {
    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");
      setAlias(tx, "node3", "alice");
      tx.commit();
    }

    try (Transaction tx = client.newTransaction()) {
      addEdge(tx, "node1", "node2");
      tx.commit();
    }

    TransactorNode tn =
        partiallyCommit(tx -> addEdge(tx, "node1", "node3"), false, closeTransactor);

    Assert.assertEquals(ImmutableSet.of("bob:joe", "joe:bob"), getDerivedEdges());

    retryOnce(tx -> setAlias(tx, "node1", "bobby"));

    Assert.assertEquals(ImmutableSet.of("bobby:joe", "joe:bobby"), getDerivedEdges());

    retryOnce(tx -> setAlias(tx, "node3", "alex"));

    Assert.assertEquals(ImmutableSet.of("bobby:joe", "joe:bobby"), getDerivedEdges());

    if (!closeTransactor) {
      tn.close();
    }
  }

  @Test
  public void testBasicRollback1() throws Exception {
    testBasicRollback(true);
  }

  @Test
  public void testBasicRollback2() throws Exception {
    testBasicRollback(false);
  }

  private void testBasicRollforward(boolean closeTransactor) throws Exception {
    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");
      setAlias(tx, "node3", "alice");
      tx.commit();
    }

    try (Transaction tx = client.newTransaction()) {
      addEdge(tx, "node1", "node2");
      tx.commit();
    }

    TransactorNode tn = partiallyCommit(tx -> addEdge(tx, "node1", "node3"), true, closeTransactor);

    retryOnce(tx -> setAlias(tx, "node1", "bobby"));

    Assert.assertEquals(ImmutableSet.of("bobby:joe", "joe:bobby", "bobby:alice", "alice:bobby"),
        getDerivedEdges());

    retryOnce(tx -> setAlias(tx, "node3", "alex"));

    Assert.assertEquals(ImmutableSet.of("bobby:joe", "joe:bobby", "bobby:alex", "alex:bobby"),
        getDerivedEdges());

    if (!closeTransactor) {
      tn.close();
    }
  }

  @Test
  public void testBasicRollforward1() throws Exception {
    testBasicRollforward(false);
  }

  @Test
  public void testBasicRollforward2() throws Exception {
    testBasicRollforward(true);
  }

  private void testParallelScan(boolean closeTransactor) throws Exception {
    Column crCol = new Column("stat", "completionRatio");

    try (Transaction tx = client.newTransaction()) {
      tx.set("user5", crCol, "0.5");
      tx.set("user6", crCol, "0.75");
      tx.commit();
    }

    TransactorNode tn = partiallyCommit(tx -> {
      // get multiple read locks with a parallel scan
      Map<String, Map<Column, String>> ratios =
          tx.withReadLock().gets(Arrays.asList("user5", "user6"), crCol);

      double cr1 = Double.parseDouble(ratios.get("user5").get(crCol));
      double cr2 = Double.parseDouble(ratios.get("user5").get(crCol));

      tx.set("org1", crCol, (cr1 + cr2) / 2 + "");
    }, false, closeTransactor);

    retryTwice(tx -> {
      Map<String, Map<Column, String>> ratios = tx.gets(Arrays.asList("user5", "user6"), crCol);

      tx.set("user5", crCol, "0.51");
      tx.set("user6", crCol, "0.76");
    });

    try (Snapshot snap = client.newSnapshot()) {
      Assert.assertNull(snap.gets("org1", crCol));
      Assert.assertEquals("0.51", snap.gets("user5", crCol));
      Assert.assertEquals("0.76", snap.gets("user6", crCol));
    }

    if (!closeTransactor) {
      tn.close();
    }
  }

  @Test
  public void testParallelScan1() throws Exception {
    testParallelScan(true);
  }

  @Test
  public void testParallelScan2() throws Exception {
    testParallelScan(false);
  }

  private void testParallelScanRC(boolean closeTransactor) throws Exception {
    // currently get w/ RowColumn has a different code path than other gets that take multiple rows
    // and columns

    Column crCol = new Column("stat", "completionRatio");

    try (Transaction tx = client.newTransaction()) {
      tx.set("user5", crCol, "0.5");
      tx.set("user6", crCol, "0.75");
      tx.commit();
    }

    TransactorNode tn = partiallyCommit(tx -> {
      // get multiple read locks with a parallel scan
      Map<RowColumn, String> ratios = tx.withReadLock()
          .gets(Arrays.asList(new RowColumn("user5", crCol), new RowColumn("user6", crCol)));


      double cr1 = Double.parseDouble(ratios.get(new RowColumn("user5", crCol)));
      double cr2 = Double.parseDouble(ratios.get(new RowColumn("user6", crCol)));

      tx.set("org1", crCol, (cr1 + cr2) / 2 + "");
    }, false, true);

    retryTwice(tx -> {
      Map<RowColumn, String> ratios =
          tx.gets(Arrays.asList(new RowColumn("user5", crCol), new RowColumn("user6", crCol)));

      tx.set("user5", crCol, "0.51");
      tx.set("user6", crCol, "0.76");
    });

    try (Snapshot snap = client.newSnapshot()) {
      Assert.assertNull(snap.gets("org1", crCol));
      Assert.assertEquals("0.51", snap.gets("user5", crCol));
      Assert.assertEquals("0.76", snap.gets("user6", crCol));
    }

    if (!closeTransactor) {
      tn.close();
    }
  }

  @Test
  public void testParallelScanRC1() throws Exception {
    testParallelScanRC(true);
  }

  @Test
  public void testParallelScanRC2() throws Exception {
    testParallelScanRC(false);
  }

  private void testWriteWoRead(boolean commitPrimary, boolean closeTransactor) throws Exception {
    // Reads can cause locks to be recovered. This test the case of a transactions that only does a
    // write to a field that has an open read lock.

    try (Transaction tx = client.newTransaction()) {
      tx.set("r1", new Column("f1", "q1"), "v1");
      tx.set("r2", new Column("f1", "q1"), "v2");
      tx.commit();
    }

    TransactorNode transactor = partiallyCommit(tx -> {
      String v1 = tx.withReadLock().gets("r1", new Column("f1", "q1"));
      String v2 = tx.withReadLock().gets("r2", new Column("f1", "q1"));

      tx.set("r3", new Column("f1", "qa"), v1 + ":" + v2);
    }, commitPrimary, closeTransactor);

    // TODO open an issue... does not really need to retry in this case
    retryOnce(tx -> {
      tx.set("r1", new Column("f1", "q1"), "v3");
    });

    try (Transaction tx = client.newTransaction()) {
      if (commitPrimary) {
        Assert.assertEquals("v1:v2", tx.gets("r3", new Column("f1", "qa")));
      } else {
        Assert.assertNull(tx.gets("r3", new Column("f1", "qa")));
      }
      Assert.assertEquals("v3", tx.gets("r1", new Column("f1", "q1")));
    }

    if (!closeTransactor) {
      transactor.close();
    }
  }

  @Test
  public void testWriteWoRead1() throws Exception {
    testWriteWoRead(false, false);
  }

  @Test
  public void testWriteWoRead2() throws Exception {
    testWriteWoRead(false, true);
  }

  @Test
  public void testWriteWoRead3() throws Exception {
    testWriteWoRead(true, false);
  }

  @Test
  public void testWriteWoRead4() throws Exception {
    testWriteWoRead(true, true);
  }

  private int countInTable(String str) throws TableNotFoundException {
    int count = 0;
    Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
    for (String e : Iterables.transform(scanner, FluoFormatter::toString)) {
      if (e.contains(str)) {
        count++;
      }
    }

    return count;
  }

  @Test
  public void testFailDeletesReadLocks() throws Exception {
    try (Transaction tx = client.newTransaction()) {
      for (int i = 0; i < 20; i++) {
        tx.set("r-" + i, new Column("f1", "q1"), "" + i);
      }

      tx.commit();
    }

    long startTs = 0;

    try (Transaction tx1 = client.newTransaction()) {
      tx1.set("r-5", new Column("f1", "q1"), "9");
      try (Transaction tx2 = client.newTransaction()) {
        tx1.commit();

        int sum = 0;
        for (int i = 0; i < 20; i++) {
          sum += Integer.parseInt(tx2.withReadLock().gets("r-" + i, new Column("f1", "q1")));
        }

        tx2.set("sum1", new Column("f", "s"), "" + sum);
        startTs = tx2.getStartTimestamp();
        tx2.commit();
        Assert.fail();
      } catch (CommitException e) {

      }
    }

    try (Snapshot snapshot = client.newSnapshot()) {
      Assert.assertNull(snapshot.gets("sum1", new Column("f", "s")));
    }

    // ensure the failed tx deleted its read locks....
    Assert.assertEquals(19, countInTable(startTs + "-RLOCK"));
    Assert.assertEquals(19, countInTable(startTs + "-DEL_RLOCK"));

    try (Transaction tx = client.newTransaction()) {
      int sum = 0;
      for (int i = 0; i < 20; i++) {
        sum += Integer.parseInt(tx.withReadLock().gets("r-" + i, new Column("f1", "q1")));
      }

      tx.set("sum1", new Column("f", "s"), "" + sum);
      tx.commit();
    }

    try (Snapshot snapshot = client.newSnapshot()) {
      Assert.assertEquals("194", snapshot.gets("sum1", new Column("f", "s")));
    }
  }
}
