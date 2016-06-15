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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverConfiguration;
import org.apache.fluo.api.config.ScannerConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.api.iterator.ColumnIterator;
import org.apache.fluo.api.iterator.RowIterator;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;
import org.apache.fluo.api.types.TypedObserver;
import org.apache.fluo.api.types.TypedTransactionBase;
import org.apache.fluo.core.exceptions.AlreadyAcknowledgedException;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

public class FluoIT extends ITBaseImpl {

  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  public static class BalanceObserver extends TypedObserver {

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(typeLayer.bc().fam("account").qual("balance").vis(),
          NotificationType.STRONG);
    }

    @Override
    public void process(TypedTransactionBase tx, Bytes row, Column col) {
      Assert.fail();
    }
  }

  @Override
  protected List<org.apache.fluo.api.config.ObserverConfiguration> getObservers() {
    return Arrays.asList(new ObserverConfiguration(BalanceObserver.class.getName()));
  };

  @Test
  public void testFluoFactory() throws Exception {
    try (FluoAdmin admin = FluoFactory.newAdmin(config)) {
      Assert.assertNotNull(admin);
    }

    try (FluoClient client = FluoFactory.newClient(config)) {
      Assert.assertNotNull(client);
      Assert.assertNotNull(client.newLoaderExecutor());

      try (Snapshot s = client.newSnapshot()) {
        Assert.assertNotNull(s);
        s.get(Bytes.of("test"), new Column(Bytes.of("cf"), Bytes.of("cq")));
      }
    }
  }

  @Test
  public void testOverlap1() throws Exception {
    // test transactions that overlap reads and both attempt to write
    // TX1 starts
    // TX2 starts
    // TX1 reads/writes
    // TX2 reads/writes
    // TX2 commits -- succeeds
    // TX1 commits -- fails

    TestTransaction tx = new TestTransaction(env);

    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis();

    tx.mutate().row("bob").col(balanceCol).set(10);
    tx.mutate().row("joe").col(balanceCol).set(20);
    tx.mutate().row("jill").col(balanceCol).set(60);

    tx.done();

    tx = new TestTransaction(env);

    int bal1 = tx.get().row("bob").col(balanceCol).toInteger(0);
    int bal2 = tx.get().row("joe").col(balanceCol).toInteger(0);
    Assert.assertEquals(10, bal1);
    Assert.assertEquals(20, bal2);

    tx.mutate().row("bob").col(balanceCol).set(bal1 - 5);
    tx.mutate().row("joe").col(balanceCol).set(bal2 + 5);

    TestTransaction tx2 = new TestTransaction(env);

    int bal3 = tx2.get().row("bob").col(balanceCol).toInteger(0);
    int bal4 = tx2.get().row("jill").col(balanceCol).toInteger(0);
    Assert.assertEquals(10, bal3);
    Assert.assertEquals(60, bal4);

    tx2.mutate().row("bob").col(balanceCol).set(bal3 - 5);
    tx2.mutate().row("jill").col(balanceCol).set(bal4 + 5);

    tx2.done();
    assertCommitFails(tx);

    TestTransaction tx3 = new TestTransaction(env);

    Assert.assertEquals(5, tx3.get().row("bob").col(balanceCol).toInteger(0));
    Assert.assertEquals(20, tx3.get().row("joe").col(balanceCol).toInteger(0));
    Assert.assertEquals(65, tx3.get().row("jill").col(balanceCol).toInteger(0));
    tx3.done();
  }

  private void assertCommitFails(TestTransaction tx) {
    try {
      tx.done();
      Assert.fail();
    } catch (CommitException ce) {
    }
  }

  private void assertAAck(TestTransaction tx) {
    try {
      tx.done();
      Assert.fail();
    } catch (AlreadyAcknowledgedException ce) {
    }
  }

  @Test
  public void testSnapshots() throws Exception {
    // test the following case
    // TX1 starts
    // TX2 starts
    // TX2 reads/writes
    // TX2 commits
    // TX1 reads -- should not see TX2 writes

    TestTransaction tx = new TestTransaction(env);

    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis();

    tx.mutate().row("bob").col(balanceCol).set(10);
    tx.mutate().row("joe").col(balanceCol).set(20);
    tx.mutate().row("jill").col(balanceCol).set(60);
    tx.mutate().row("jane").col(balanceCol).set(0);

    tx.done();

    TestTransaction tx1 = new TestTransaction(env);

    TestTransaction tx2 = new TestTransaction(env);

    tx2.mutate().row("bob").col(balanceCol).set(tx2.get().row("bob").col(balanceCol).toLong() - 5);
    tx2.mutate().row("joe").col(balanceCol).set(tx2.get().row("joe").col(balanceCol).toLong() - 5);
    tx2.mutate().row("jill").col(balanceCol)
        .set(tx2.get().row("jill").col(balanceCol).toLong() + 10);

    long bal1 = tx1.get().row("bob").col(balanceCol).toLong();

    tx2.done();

    TestTransaction txd = new TestTransaction(env);
    txd.mutate().row("jane").col(balanceCol).delete();
    txd.done();

    long bal2 = tx1.get().row("joe").col(balanceCol).toLong();
    long bal3 = tx1.get().row("jill").col(balanceCol).toLong();
    long bal4 = tx1.get().row("jane").col(balanceCol).toLong();

    Assert.assertEquals(10l, bal1);
    Assert.assertEquals(20l, bal2);
    Assert.assertEquals(60l, bal3);
    Assert.assertEquals(0l, bal4);

    tx1.mutate().row("bob").col(balanceCol).set(bal1 - 5);
    tx1.mutate().row("joe").col(balanceCol).set(bal2 + 5);

    assertCommitFails(tx1);

    TestTransaction tx3 = new TestTransaction(env);

    TestTransaction tx4 = new TestTransaction(env);
    tx4.mutate().row("jane").col(balanceCol).set(3);
    tx4.done();

    Assert.assertEquals(5l, tx3.get().row("bob").col(balanceCol).toLong(0));
    Assert.assertEquals(15l, tx3.get().row("joe").col(balanceCol).toLong(0));
    Assert.assertEquals(70l, tx3.get().row("jill").col(balanceCol).toLong(0));
    Assert.assertNull(tx3.get().row("jane").col(balanceCol).toLong());
    tx3.done();

    TestTransaction tx5 = new TestTransaction(env);

    Assert.assertEquals(5l, tx5.get().row("bob").col(balanceCol).toLong(0));
    Assert.assertEquals(15l, tx5.get().row("joe").col(balanceCol).toLong(0));
    Assert.assertEquals(70l, tx5.get().row("jill").col(balanceCol).toLong(0));
    Assert.assertEquals(3l, tx5.get().row("jane").col(balanceCol).toLong(0));
    tx5.done();
  }

  @Test
  public void testAck() throws Exception {
    // when two transactions run against the same observed column, only one should commit

    TestTransaction tx = new TestTransaction(env);

    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis();

    tx.mutate().row("bob").col(balanceCol).set(10);
    tx.mutate().row("joe").col(balanceCol).set(20);
    tx.mutate().row("jill").col(balanceCol).set(60);

    tx.done();

    TestTransaction tx1 = new TestTransaction(env, "joe", balanceCol);
    tx1.get().row("joe").col(balanceCol);
    tx1.mutate().row("jill").col(balanceCol).set(61);

    TestTransaction tx2 = new TestTransaction(env, "joe", balanceCol);
    tx2.get().row("joe").col(balanceCol);
    tx2.mutate().row("bob").col(balanceCol).set(11);

    tx1.done();
    assertAAck(tx2);

    TestTransaction tx3 = new TestTransaction(env);

    Assert.assertEquals(10, tx3.get().row("bob").col(balanceCol).toInteger(0));
    Assert.assertEquals(20, tx3.get().row("joe").col(balanceCol).toInteger(0));
    Assert.assertEquals(61, tx3.get().row("jill").col(balanceCol).toInteger(0));

    // update joe, so it can be acknowledged again
    tx3.mutate().row("joe").col(balanceCol).set(21);

    tx3.done();

    TestTransaction tx4 = new TestTransaction(env, "joe", balanceCol);
    tx4.get().row("joe").col(balanceCol);
    tx4.mutate().row("jill").col(balanceCol).set(62);

    TestTransaction tx5 = new TestTransaction(env, "joe", balanceCol);
    tx5.get().row("joe").col(balanceCol);
    tx5.mutate().row("bob").col(balanceCol).set(11);

    TestTransaction tx7 = new TestTransaction(env, "joe", balanceCol);

    // make the 2nd transaction to start commit 1st
    tx5.done();
    assertAAck(tx4);

    TestTransaction tx6 = new TestTransaction(env);

    Assert.assertEquals(11, tx6.get().row("bob").col(balanceCol).toInteger(0));
    Assert.assertEquals(21, tx6.get().row("joe").col(balanceCol).toInteger(0));
    Assert.assertEquals(61, tx6.get().row("jill").col(balanceCol).toInteger(0));
    tx6.done();

    tx7.get().row("joe").col(balanceCol);
    tx7.mutate().row("bob").col(balanceCol).set(15);
    tx7.mutate().row("jill").col(balanceCol).set(60);

    assertAAck(tx7);

    TestTransaction tx8 = new TestTransaction(env);

    Assert.assertEquals(11, tx8.get().row("bob").col(balanceCol).toInteger(0));
    Assert.assertEquals(21, tx8.get().row("joe").col(balanceCol).toInteger(0));
    Assert.assertEquals(61, tx8.get().row("jill").col(balanceCol).toInteger(0));
    tx8.done();
  }

  @Test
  public void testAck2() throws Exception {
    TestTransaction tx = new TestTransaction(env);

    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis();
    Column addrCol = typeLayer.bc().fam("account").qual("addr").vis();

    tx.mutate().row("bob").col(balanceCol).set(10);
    tx.mutate().row("joe").col(balanceCol).set(20);
    tx.mutate().row("jill").col(balanceCol).set(60);

    tx.done();

    TestTransaction tx1 = new TestTransaction(env, "bob", balanceCol);
    TestTransaction tx2 = new TestTransaction(env, "bob", balanceCol);
    TestTransaction tx3 = new TestTransaction(env, "bob", balanceCol);

    tx1.get().row("bob").col(balanceCol).toInteger();
    tx2.get().row("bob").col(balanceCol).toInteger();

    tx1.get().row("bob").col(addrCol).toInteger();
    tx2.get().row("bob").col(addrCol).toInteger();

    tx1.mutate().row("bob").col(addrCol).set("1 loop pl");
    tx2.mutate().row("bob").col(addrCol).set("1 loop pl");

    // this test overlaps the commits of two transactions w/ the same trigger

    CommitData cd = tx1.createCommitData();
    Assert.assertTrue(tx1.preCommit(cd));

    assertCommitFails(tx2);

    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(tx1.commitPrimaryColumn(cd, commitTs));
    tx1.finishCommit(cd, commitTs);
    tx1.close();

    tx3.mutate().row("bob").col(addrCol).set("2 loop pl");
    assertAAck(tx3);
  }

  @Test
  public void testAck3() throws Exception {
    TestTransaction tx = new TestTransaction(env);

    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis();

    tx.mutate().row("bob").col(balanceCol).set(10);
    tx.mutate().row("joe").col(balanceCol).set(20);
    tx.mutate().row("jill").col(balanceCol).set(60);

    tx.done();

    long notTS1 = TestTransaction.getNotificationTS(env, "bob", balanceCol);

    // this transaction should create a second notification
    TestTransaction tx1 = new TestTransaction(env);
    tx1.mutate().row("bob").col(balanceCol).set(11);
    tx1.done();

    long notTS2 = TestTransaction.getNotificationTS(env, "bob", balanceCol);

    Assert.assertTrue(notTS1 < notTS2);

    // even though there were two notifications and TX is using 1st notification TS... only 1st TX
    // should execute
    // google paper calls this message collapsing

    TestTransaction tx3 = new TestTransaction(env, "bob", balanceCol, notTS1);

    TestTransaction tx2 = new TestTransaction(env, "bob", balanceCol, notTS1);
    Assert.assertEquals(11, tx2.get().row("bob").col(balanceCol).toInteger(0));
    tx2.done();

    Assert.assertEquals(11, tx3.get().row("bob").col(balanceCol).toInteger(0));
    assertAAck(tx3);

    TestTransaction tx4 = new TestTransaction(env, "bob", balanceCol, notTS2);
    Assert.assertEquals(11, tx4.get().row("bob").col(balanceCol).toInteger(0));
    assertAAck(tx4);
  }

  @Test
  public void testWriteObserved() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged
    // status

    TestTransaction tx = new TestTransaction(env);

    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis();

    tx.mutate().row("bob").col(balanceCol).set(10);
    tx.mutate().row("joe").col(balanceCol).set(20);
    tx.mutate().row("jill").col(balanceCol).set(60);

    tx.done();

    TestTransaction tx2 = new TestTransaction(env, "joe", balanceCol);
    tx2.get().row("joe").col(balanceCol);
    tx2.mutate().row("joe").col(balanceCol).set(21);
    tx2.mutate().row("bob").col(balanceCol).set(11);

    TestTransaction tx1 = new TestTransaction(env, "joe", balanceCol);
    tx1.get().row("joe").col(balanceCol);
    tx1.mutate().row("jill").col(balanceCol).set(61);

    tx1.done();
    assertAAck(tx2);

    TestTransaction tx3 = new TestTransaction(env);

    Assert.assertEquals(10, tx3.get().row("bob").col(balanceCol).toInteger(0));
    Assert.assertEquals(20, tx3.get().row("joe").col(balanceCol).toInteger(0));
    Assert.assertEquals(61, tx3.get().row("jill").col(balanceCol).toInteger(0));

    tx3.done();
  }

  @Test
  public void testVisibility() throws Exception {

    conn.securityOperations().changeUserAuthorizations(USER, new Authorizations("A", "B", "C"));

    env.setAuthorizations(new Authorizations("A", "B", "C"));

    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis("A|B");

    TestTransaction tx = new TestTransaction(env);

    tx.mutate().row("bob").col(balanceCol).set(10);
    tx.mutate().row("joe").col(balanceCol).set(20);
    tx.mutate().row("jill").col(balanceCol).set(60);

    tx.done();

    FluoConfiguration fc = new FluoConfiguration(config);
    Environment env2 = new Environment(fc);
    env2.setAuthorizations(new Authorizations("B"));

    TestTransaction tx2 = new TestTransaction(env2);
    Assert.assertEquals(10, tx2.get().row("bob").col(balanceCol).toInteger(0));
    Assert.assertEquals(20, tx2.get().row("joe").col(balanceCol).toInteger(0));
    Assert.assertEquals(60, tx2.get().row("jill").col(balanceCol).toInteger(0));
    tx2.done();
    env2.close();

    Environment env3 = new Environment(fc);
    env3.setAuthorizations(new Authorizations("C"));

    TestTransaction tx3 = new TestTransaction(env3);
    Assert.assertNull(tx3.get().row("bob").col(balanceCol).toInteger());
    Assert.assertNull(tx3.get().row("joe").col(balanceCol).toInteger());
    Assert.assertNull(tx3.get().row("jill").col(balanceCol).toInteger());
    tx3.done();
    env3.close();
  }

  @Test
  public void testRange() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged
    // status

    TestTransaction tx = new TestTransaction(env);
    tx.mutate().row("d00001").fam("data").qual("content")
        .set("blah blah, blah http://a.com. Blah blah http://b.com.  Blah http://c.com");
    tx.mutate().row("d00001").fam("outlink").qual("http://a.com").set("");
    tx.mutate().row("d00001").fam("outlink").qual("http://b.com").set("");
    tx.mutate().row("d00001").fam("outlink").qual("http://c.com").set("");

    tx.mutate().row("d00002").fam("data").qual("content")
        .set("blah blah, blah http://d.com. Blah blah http://e.com.  Blah http://c.com");
    tx.mutate().row("d00002").fam("outlink").qual("http://d.com").set("");
    tx.mutate().row("d00002").fam("outlink").qual("http://e.com").set("");
    tx.mutate().row("d00002").fam("outlink").qual("http://c.com").set("");

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);

    TestTransaction tx3 = new TestTransaction(env);

    tx3.mutate().row("d00001").fam("data").qual("content")
        .set("blah blah, blah http://a.com. Blah http://c.com .  Blah http://z.com");
    tx3.mutate().row("d00001").fam("outlink").qual("http://a.com").set("");
    tx3.mutate().row("d00001").fam("outlink").qual("http://b.com").delete();
    tx3.mutate().row("d00001").fam("outlink").qual("http://c.com").set("");
    tx3.mutate().row("d00001").fam("outlink").qual("http://z.com").set("");

    tx3.done();

    HashSet<Column> columns = new HashSet<>();
    RowIterator riter =
        tx2.get(new ScannerConfiguration().setSpan(Span.exact(Bytes.of("d00001"),
            new Column(Bytes.of("outlink")))));
    while (riter.hasNext()) {
      ColumnIterator citer = riter.next().getValue();
      while (citer.hasNext()) {
        columns.add(citer.next().getKey());
      }
    }

    tx2.done();

    HashSet<Column> expected = new HashSet<>();
    expected.add(typeLayer.bc().fam("outlink").qual("http://a.com").vis());
    expected.add(typeLayer.bc().fam("outlink").qual("http://b.com").vis());
    expected.add(typeLayer.bc().fam("outlink").qual("http://c.com").vis());

    Assert.assertEquals(expected, columns);

    TestTransaction tx4 = new TestTransaction(env);
    columns.clear();
    riter =
        tx4.get(new ScannerConfiguration().setSpan(Span.exact(Bytes.of("d00001"),
            new Column(Bytes.of("outlink")))));
    while (riter.hasNext()) {
      ColumnIterator citer = riter.next().getValue();
      while (citer.hasNext()) {
        columns.add(citer.next().getKey());
      }
    }
    expected.add(typeLayer.bc().fam("outlink").qual("http://z.com").vis());
    expected.remove(typeLayer.bc().fam("outlink").qual("http://b.com").vis());
    Assert.assertEquals(expected, columns);
    tx4.done();
  }

  @Test
  public void testStringMethods() {
    TestTransaction tx = new TestTransaction(env);

    Column ccol = new Column("doc", "content");
    Column tcol = new Column("doc", "time");

    tx.set("d:0001", ccol, "abc def");
    tx.set("d:0001", tcol, "45");
    tx.set("d:0002", ccol, "neb feg");
    tx.set("d:0002", tcol, "97");
    tx.set("d:0003", ccol, "xyz abc");
    tx.set("d:0003", tcol, "42");

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);

    Map<String, Map<Column, String>> map1 =
        tx2.gets(Arrays.asList("d:0001", "d:0002"), Collections.singleton(ccol));
    Map<String, ImmutableMap<Column, String>> expected1 =
        ImmutableMap.of("d:0001", ImmutableMap.of(ccol, "abc def"), "d:0002",
            ImmutableMap.of(ccol, "neb feg"));
    Assert.assertEquals(expected1, map1);

    Assert.assertEquals("45", tx2.gets("d:0001", tcol));
    Assert.assertEquals("xyz abc", tx2.gets("d:0003", ccol));

    Map<Column, String> map2 = tx2.gets("d:0002", ImmutableSet.of(ccol, tcol));
    Map<Column, String> expected2 = ImmutableMap.of(ccol, "neb feg", tcol, "97");
    Assert.assertEquals(expected2, map2);

    tx2.delete("d:0003", ccol);
    tx2.delete("d:0003", tcol);

    tx2.done();

    TestTransaction tx3 = new TestTransaction(env);

    Assert.assertNull(tx3.gets("d:0003", ccol));
    Assert.assertNull(tx3.gets("d:0003", tcol));

    tx3.done();
  }
}
