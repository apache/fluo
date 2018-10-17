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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.accumulo.format.FluoFormatter;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.AlreadySetException;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

import static java.util.Arrays.asList;

public class ReadLockIT extends ITBaseImpl {

  private static final Column ALIAS_COL = new Column("node", "alias");

  private void addEdge(String node1, String node2) {
    try (Transaction tx = client.newTransaction()) {
      addEdge(tx, node1, node2);
      tx.commit();
    }
  }

  static void addEdge(TransactionBase tx, String node1, String node2) {
    Map<String, Map<Column, String>> aliases =
        tx.withReadLock().gets(asList("r:" + node1, "r:" + node2), ALIAS_COL);
    String alias1 = aliases.get("r:" + node1).get(ALIAS_COL);
    String alias2 = aliases.get("r:" + node2).get(ALIAS_COL);

    addEdge(tx, node1, node2, alias1, alias2);
  }

  static void addEdge(TransactionBase tx, String node1, String node2, String alias1,
      String alias2) {
    tx.set("d:" + alias1 + ":" + alias2, new Column("edge", node1 + ":" + node2), "");
    tx.set("d:" + alias2 + ":" + alias1, new Column("edge", node2 + ":" + node1), "");

    tx.set("r:" + node1 + ":" + node2, new Column("edge", "aliases"), alias1 + ":" + alias2);
    tx.set("r:" + node2 + ":" + node1, new Column("edge", "aliases"), alias2 + ":" + alias1);
  }

  static void setAlias(TransactionBase tx, String node, String alias) {
    tx.set("r:" + node, new Column("node", "alias"), alias);

    CellScanner scanner = tx.scanner().over(Span.prefix("r:" + node + ":")).build();

    for (RowColumnValue rcv : scanner) {
      String otherNode = rcv.getsRow().split(":")[2];
      String[] aliases = rcv.getsValue().split(":");

      if (aliases.length != 2) {
        throw new RuntimeException("bad alias " + rcv);
      }

      if (!alias.equals(aliases[0])) {
        tx.delete("d:" + aliases[0] + ":" + aliases[1], new Column("edge", node + ":" + otherNode));
        tx.delete("d:" + aliases[1] + ":" + aliases[0], new Column("edge", otherNode + ":" + node));

        addEdge(tx, node, otherNode, alias, aliases[1]);
      }
    }
  }

  @Test
  public void testConcurrentReadlocks() throws Exception {

    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");
      setAlias(tx, "node3", "alice");
      tx.commit();
    }


    TestTransaction tx1 = new TestTransaction(env);
    setAlias(tx1, "node2", "jojo");

    TestTransaction tx2 = new TestTransaction(env);
    TestTransaction tx3 = new TestTransaction(env);

    addEdge(tx2, "node1", "node2");
    addEdge(tx3, "node1", "node3");

    tx2.commit();
    tx2.close();

    tx3.commit();
    tx3.close();

    Assert.assertEquals(ImmutableSet.of("bob:joe", "joe:bob", "bob:alice", "alice:bob"),
        getDerivedEdges());

    try {
      tx1.commit();
      Assert.fail("Expected exception");
    } catch (CommitException ce) {
      // expected
    }

    Assert.assertEquals(ImmutableSet.of("bob:joe", "joe:bob", "bob:alice", "alice:bob"),
        getDerivedEdges());
  }

  @Test
  public void testWriteCausesReadLockToFail() throws Exception {
    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");
      tx.commit();
    }

    TestTransaction tx1 = new TestTransaction(env);
    setAlias(tx1, "node2", "jojo");

    TestTransaction tx2 = new TestTransaction(env);

    addEdge(tx2, "node1", "node2");

    tx1.commit();
    tx1.close();

    Assert.assertEquals(0, getDerivedEdges().size());

    try {
      tx2.commit();
      Assert.fail("Expected exception");
    } catch (CommitException ce) {
      // expected
    }

    // ensure the failed read lock on node1 is cleaned up
    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "fred");
      tx.commit();
    }

    try (Transaction tx = client.newTransaction()) {
      addEdge(tx, "node1", "node2");
      tx.commit();
    }

    Assert.assertEquals(ImmutableSet.of("fred:jojo", "jojo:fred"), getDerivedEdges());
  }

  private void dumpRow(String row, Consumer<String> out) throws TableNotFoundException {
    Scanner scanner = aClient.createScanner(getCurTableName(), Authorizations.EMPTY);
    scanner.setRange(Range.exact(row));
    for (Entry<Key, Value> entry : scanner) {
      out.accept(FluoFormatter.toString(entry));
    }
  }

  private void dumpTable(Consumer<String> out) throws TableNotFoundException {
    Scanner scanner = aClient.createScanner(getCurTableName(), Authorizations.EMPTY);
    for (Entry<Key, Value> entry : scanner) {
      out.accept(FluoFormatter.toString(entry));
    }
  }

  @Test
  public void testWriteAfterReadLock() throws Exception {
    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");
      setAlias(tx, "node3", "alice");

      tx.commit();
    }

    addEdge("node1", "node2");
    Assert.assertEquals(ImmutableSet.of("bob:joe", "joe:bob"), getDerivedEdges());

    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bobby");
      tx.commit();
    }

    addEdge("node1", "node3");
    Assert.assertEquals(ImmutableSet.of("bobby:joe", "joe:bobby", "bobby:alice", "alice:bobby"),
        getDerivedEdges());
  }

  @Test
  public void testRandom() throws Exception {
    final int numNodes = 100;
    final int numEdges = 1000;
    final int numAliasChanges = 25;

    Random rand = new Random();

    Map<String, String> nodes = new HashMap<>();
    while (nodes.size() < numNodes) {
      nodes.put(String.format("n-%09d", rand.nextInt(1000000000)),
          String.format("a-%09d", rand.nextInt(1000000000)));
    }

    List<String> nodesList = new ArrayList<>(nodes.keySet());
    Set<String> edges = new HashSet<>();
    while (edges.size() < numEdges) {
      String n1 = nodesList.get(rand.nextInt(nodesList.size()));
      String n2 = nodesList.get(rand.nextInt(nodesList.size()));
      if (n1.equals(n2) || edges.contains(n2 + ":" + n1)) {
        continue;
      }

      edges.add(n1 + ":" + n2);
    }

    try (LoaderExecutor le = client.newLoaderExecutor()) {
      for (Entry<String, String> entry : nodes.entrySet()) {
        le.execute((tx, ctx) -> setAlias(tx, entry.getKey(), entry.getValue()));
      }
    }

    List<Loader> loadOps = new ArrayList<>();
    for (String edge : edges) {
      String[] enodes = edge.split(":");
      loadOps.add((tx, ctx) -> {
        try {
          addEdge(tx, enodes[0], enodes[1]);
        } catch (NullPointerException e) {
          // TODO remove after finding bug
          System.out.println(
              " en0 " + enodes[0] + " en1 " + enodes[1] + " start ts " + tx.getStartTimestamp());
          dumpRow("r:" + enodes[0], System.out::println);
          dumpRow("r:" + enodes[1], System.out::println);
          throw e;
        }
      });
    }

    Map<String, String> changes = new HashMap<>();
    while (changes.size() < numAliasChanges) {
      String node = nodesList.get(rand.nextInt(nodesList.size()));
      String alias = String.format("a-%09d", rand.nextInt(1000000000));
      changes.put(node, alias);
    }

    Map<String, String> nodes2 = new HashMap<>(nodes);
    nodes2.putAll(changes);

    changes.forEach((node, alias) -> {
      loadOps.add((tx, ctx) -> setAlias(tx, node, alias));
    });

    Collections.shuffle(loadOps, rand);

    FluoConfiguration conf = new FluoConfiguration(config);
    conf.setLoaderThreads(20);
    try (FluoClient client = FluoFactory.newClient(conf);
        LoaderExecutor le = client.newLoaderExecutor()) {
      loadOps.forEach(loader -> le.execute(loader));
    }

    Set<String> expectedEdges = new HashSet<>();
    for (String edge : edges) {
      String[] enodes = edge.split(":");
      String alias1 = nodes2.get(enodes[0]);
      String alias2 = nodes2.get(enodes[1]);

      expectedEdges.add(alias1 + ":" + alias2);
      expectedEdges.add(alias2 + ":" + alias1);
    }

    Set<String> actualEdges = getDerivedEdges();

    if (!expectedEdges.equals(actualEdges)) {
      Path dumpFile = Paths.get("target/ReadLockIT.txt");

      try (BufferedWriter writer = Files.newBufferedWriter(dumpFile)) {

        Consumer<String> out = s -> {
          try {
            writer.append(s);
            writer.append("\n");
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        };


        writer.append("Alias changes : \n");
        Maps.difference(nodes, nodes2).entriesDiffering()
            .forEach((k, v) -> out.accept(k + " " + v));

        writer.append("expected - actual : \n");
        Sets.difference(expectedEdges, actualEdges).forEach(out);
        writer.append("\n");

        writer.append("actual - expected : \n");
        Sets.difference(actualEdges, expectedEdges).forEach(out);
        writer.append("\n");

        printSnapshot(out);
        dumpTable(out);
      }
      Assert.fail("Did not produce expected graph, dumped info to " + dumpFile);
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

  @Test(expected = AlreadySetException.class)
  public void testReadAndWriteLockInSameTx() {
    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bob");
      setAlias(tx, "node2", "joe");


      tx.commit();
    }

    try (Transaction tx = client.newTransaction()) {
      setAlias(tx, "node1", "bobby");
      // tries to get a read lock on node1 in same tx
      addEdge(tx, "node1", "node2");
    }
  }

  @Test(expected = AlreadySetException.class)
  public void testReadAndDeleteInSameTx() {
    try (Transaction tx = client.newTransaction()) {
      tx.set("123456", new Column("f", "q"), "abc");
      tx.commit();
    }

    try (Transaction tx = client.newTransaction()) {
      tx.delete("123456", new Column("f", "q"));
      // should fail here because already have write lock
      String val = tx.withReadLock().gets("123456", new Column("f", "q"));
      tx.set("123457", new Column("f", "q"), val + "7");
      tx.commit();
    }
  }

  private static final Column c1 = new Column("f1", "q1");
  private static final Column c2 = new Column("f1", "q2");
  private static final Column invCol = new Column("f1", "inv");

  private final void ensureReadLocksSet(Consumer<TransactionBase> readLockOperation) {

    try (Transaction txi = client.newTransaction()) {
      txi.set("test1", c1, "45");
      txi.set("test1", c2, "90");
      txi.set("test2", c1, "30");
      txi.set("test2", c2, "60");
      txi.commit();
    }


    List<Consumer<TransactionBase>> writeLockOperations = ImmutableList.of(txw -> {
      txw.set("test1", c1, "47");
    }, txw -> {
      txw.set("test1", c2, "94");
    }, txw -> {
      txw.set("test2", c1, "37");
    }, txw -> {
      txw.set("test2", c2, "74");
    });

    List<Transaction> writeTxs = new ArrayList<>();
    for (Consumer<TransactionBase> wop : writeLockOperations) {
      Transaction wtx = client.newTransaction();
      wop.accept(wtx);
      writeTxs.add(wtx);
    }

    try (Transaction txr = client.newTransaction()) {
      readLockOperation.accept(txr);
      txr.commit();
    }

    for (Transaction wtx : writeTxs) {
      try {
        wtx.commit();
        Assert.fail();
      } catch (CommitException ce) {
        // expected
      }
    }

    try (Snapshot snap = client.newSnapshot()) {
      Assert.assertEquals("45", snap.gets("test1", c1));
      Assert.assertEquals("90", snap.gets("test1", c2));
      Assert.assertEquals("test1", snap.gets("45", invCol));
      Assert.assertEquals("test1", snap.gets("90", invCol));
      Assert.assertEquals("30", snap.gets("test2", c1));
      Assert.assertEquals("60", snap.gets("test2", c2));
      Assert.assertEquals("test2", snap.gets("30", invCol));
      Assert.assertEquals("test2", snap.gets("60", invCol));
    }
  }

  @Test
  public void testGet() {
    ensureReadLocksSet(txr -> {
      // ensure this operation sets two read locks
      SnapshotBase rlSnap = txr.withReadLock();

      txr.set(rlSnap.gets("test1", c1), invCol, "test1");
      txr.set(rlSnap.gets("test1", c2), invCol, "test1");
      txr.set(rlSnap.gets("test2", c1), invCol, "test2");
      txr.set(rlSnap.gets("test2", c2), invCol, "test2");
    });
  }

  @Test
  public void testGetColumns() {
    ensureReadLocksSet(txr -> {
      // ensure this operation sets two read locks
      Map<Column, String> vals = txr.withReadLock().gets("test1", c1, c2);
      txr.set(vals.get(c1), invCol, "test1");
      txr.set(vals.get(c2), invCol, "test1");
      vals = txr.withReadLock().gets("test2", c1, c2);
      txr.set(vals.get(c1), invCol, "test2");
      txr.set(vals.get(c2), invCol, "test2");
    });
  }

  @Test
  public void testGetRowsColumns() {
    ensureReadLocksSet(txr -> {
      // ensure this operation sets two read locks
      Map<String, Map<Column, String>> vals =
          txr.withReadLock().gets(ImmutableList.of("test1", "test2"), c1, c2);
      txr.set(vals.get("test1").get(c1), invCol, "test1");
      txr.set(vals.get("test1").get(c2), invCol, "test1");
      txr.set(vals.get("test2").get(c1), invCol, "test2");
      txr.set(vals.get("test2").get(c2), invCol, "test2");
    });
  }

  @Test
  public void testGetRowColumns() {
    ensureReadLocksSet(txr -> {
      // ensure this operation sets two read locks
      Map<RowColumn, String> vals =
          txr.withReadLock().gets(ImmutableList.of(new RowColumn("test1", c1),
              new RowColumn("test1", c2), new RowColumn("test2", c1), new RowColumn("test2", c2)));
      txr.set(vals.get(new RowColumn("test1", c1)), invCol, "test1");
      txr.set(vals.get(new RowColumn("test1", c2)), invCol, "test1");
      txr.set(vals.get(new RowColumn("test2", c1)), invCol, "test2");
      txr.set(vals.get(new RowColumn("test2", c2)), invCol, "test2");
    });
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testScan() {
    try (Transaction tx = client.newTransaction()) {
      tx.withReadLock().scanner().build();
    }
  }

  @Test
  public void testOnlyReadLocks() {
    try (Transaction tx = client.newTransaction()) {
      tx.set("r1", new Column("q1", "f1"), "v1");
      tx.set("r2", new Column("q1", "f1"), "v2");
      tx.commit();
    }

    try (Transaction tx1 = client.newTransaction()) {
      try (Transaction tx2 = client.newTransaction()) {
        String v1 = tx2.withReadLock().gets("r1", new Column("q1", "f1"));
        String v2 = tx2.withReadLock().gets("r2", new Column("q1", "f1"));

        Assert.assertEquals("v1", v1);
        Assert.assertEquals("v2", v2);

        // commit should be a no-op because only read locks
        tx2.commit();
      }

      tx1.set("r1", new Column("q1", "f1"), "v3");
      tx1.set("r2", new Column("q1", "f1"), "v4");

      // should not collide with read locks
      tx1.commit();
    }
    try (Snapshot snap = client.newSnapshot()) {
      String v1 = snap.gets("r1", new Column("q1", "f1"));
      String v2 = snap.gets("r2", new Column("q1", "f1"));

      Assert.assertEquals("v3", v1);
      Assert.assertEquals("v4", v2);
    }
  }
}
