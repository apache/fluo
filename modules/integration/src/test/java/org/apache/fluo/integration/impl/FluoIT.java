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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.core.exceptions.AlreadyAcknowledgedException;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.apache.fluo.integration.TestUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.fluo.integration.BankUtil.BALANCE;

public class FluoIT extends ITBaseImpl {

  public static class FluoITObserverProvider implements ObserverProvider {
    @Override
    public void provide(Registry or, Context ctx) {
      or.forColumn(BALANCE, NotificationType.STRONG).useObserver((tx, row, col) -> {
        Assert.fail();
      });
    }
  }

  @Override
  protected Class<? extends ObserverProvider> getObserverProviderClass() {
    return FluoITObserverProvider.class;
  }

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

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");

    tx.done();

    tx = new TestTransaction(env);

    Assert.assertEquals("10", tx.gets("bob", BALANCE));
    Assert.assertEquals("20", tx.gets("joe", BALANCE));

    TestUtil.increment(tx, "bob", BALANCE, -5);
    TestUtil.increment(tx, "joe", BALANCE, 5);

    TestTransaction tx2 = new TestTransaction(env);

    Assert.assertEquals("10", tx2.gets("bob", BALANCE));
    Assert.assertEquals("60", tx2.gets("jill", BALANCE));

    TestUtil.increment(tx2, "bob", BALANCE, -5);
    TestUtil.increment(tx2, "jill", BALANCE, 5);

    tx2.done();
    assertCommitFails(tx);

    TestTransaction tx3 = new TestTransaction(env);

    Assert.assertEquals("5", tx3.gets("bob", BALANCE));
    Assert.assertEquals("20", tx3.gets("joe", BALANCE));
    Assert.assertEquals("65", tx3.gets("jill", BALANCE));
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

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");
    tx.set("jane", BALANCE, "0");

    tx.done();

    TestTransaction tx1 = new TestTransaction(env);

    TestTransaction tx2 = new TestTransaction(env);

    TestUtil.increment(tx2, "bob", BALANCE, -5);
    TestUtil.increment(tx2, "joe", BALANCE, -5);
    TestUtil.increment(tx2, "jill", BALANCE, 10);

    Assert.assertEquals("10", tx1.gets("bob", BALANCE));

    tx2.done();

    TestTransaction txd = new TestTransaction(env);
    txd.delete("jane", BALANCE);
    txd.done();

    Assert.assertEquals("20", tx1.gets("joe", BALANCE));
    Assert.assertEquals("60", tx1.gets("jill", BALANCE));
    Assert.assertEquals("0", tx1.gets("jane", BALANCE));

    tx1.set("bob", BALANCE, "5");
    tx1.set("joe", BALANCE, "25");

    assertCommitFails(tx1);

    TestTransaction tx3 = new TestTransaction(env);

    TestTransaction tx4 = new TestTransaction(env);
    tx4.set("jane", BALANCE, "3");
    tx4.done();

    Assert.assertEquals("5", tx3.gets("bob", BALANCE));
    Assert.assertEquals("15", tx3.gets("joe", BALANCE));
    Assert.assertEquals("70", tx3.gets("jill", BALANCE));
    Assert.assertNull(tx3.gets("jane", BALANCE));
    tx3.done();

    TestTransaction tx5 = new TestTransaction(env);

    Assert.assertEquals("5", tx5.gets("bob", BALANCE));
    Assert.assertEquals("15", tx5.gets("joe", BALANCE));
    Assert.assertEquals("70", tx5.gets("jill", BALANCE));
    Assert.assertEquals("3", tx5.gets("jane", BALANCE));
    tx5.done();
  }

  @Test
  public void testAck() throws Exception {
    // when two transactions run against the same observed column, only one should commit

    TestTransaction tx = new TestTransaction(env);

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");

    tx.done();

    TestTransaction tx1 = new TestTransaction(env, "joe", BALANCE);
    tx1.gets("joe", BALANCE);
    tx1.set("jill", BALANCE, "61");

    TestTransaction tx2 = new TestTransaction(env, "joe", BALANCE);
    tx2.gets("joe", BALANCE);
    tx2.set("bob", BALANCE, "11");

    tx1.done();
    assertAAck(tx2);

    TestTransaction tx3 = new TestTransaction(env);

    Assert.assertEquals("10", tx3.gets("bob", BALANCE));
    Assert.assertEquals("20", tx3.gets("joe", BALANCE));
    Assert.assertEquals("61", tx3.gets("jill", BALANCE));

    // update joe, so it can be acknowledged again
    tx3.set("joe", BALANCE, "21");

    tx3.done();

    TestTransaction tx4 = new TestTransaction(env, "joe", BALANCE);
    tx4.gets("joe", BALANCE);
    tx4.set("jill", BALANCE, "62");

    TestTransaction tx5 = new TestTransaction(env, "joe", BALANCE);
    tx5.gets("joe", BALANCE);
    tx5.set("bob", BALANCE, "11");

    TestTransaction tx7 = new TestTransaction(env, "joe", BALANCE);

    // make the 2nd transaction to start commit 1st
    tx5.done();
    assertAAck(tx4);

    TestTransaction tx6 = new TestTransaction(env);

    Assert.assertEquals("11", tx6.gets("bob", BALANCE));
    Assert.assertEquals("21", tx6.gets("joe", BALANCE));
    Assert.assertEquals("61", tx6.gets("jill", BALANCE));
    tx6.done();

    tx7.gets("joe", BALANCE);
    tx7.set("bob", BALANCE, "15");
    tx7.set("jill", BALANCE, "60");

    assertAAck(tx7);

    TestTransaction tx8 = new TestTransaction(env);

    Assert.assertEquals("11", tx8.gets("bob", BALANCE));
    Assert.assertEquals("21", tx8.gets("joe", BALANCE));
    Assert.assertEquals("61", tx8.gets("jill", BALANCE));
    tx8.done();
  }

  @Test
  public void testAck2() throws Exception {
    TestTransaction tx = new TestTransaction(env);

    Column addrCol = new Column("account", "addr");

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");

    tx.done();

    TestTransaction tx1 = new TestTransaction(env, "bob", BALANCE);
    TestTransaction tx2 = new TestTransaction(env, "bob", BALANCE);
    TestTransaction tx3 = new TestTransaction(env, "bob", BALANCE);

    tx1.gets("bob", BALANCE);
    tx2.gets("bob", BALANCE);

    tx1.gets("bob", addrCol);
    tx2.gets("bob", addrCol);

    tx1.set("bob", addrCol, "1 loop pl");
    tx2.set("bob", addrCol, "1 loop pl");

    // this test overlaps the commits of two transactions w/ the same trigger

    CommitData cd = tx1.createCommitData();
    Assert.assertTrue(tx1.preCommit(cd));

    assertCommitFails(tx2);

    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(tx1.commitPrimaryColumn(cd, commitTs));
    tx1.finishCommit(cd, commitTs);
    tx1.close();

    tx3.set("bob", addrCol, "2 loop pl");
    assertAAck(tx3);
  }

  @Test
  public void testAck3() throws Exception {
    TestTransaction tx = new TestTransaction(env);

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");

    tx.done();

    long notTS1 = TestTransaction.getNotificationTS(env, "bob", BALANCE);

    // this transaction should create a second notification
    TestTransaction tx1 = new TestTransaction(env);
    tx1.set("bob", BALANCE, "11");
    tx1.done();

    long notTS2 = TestTransaction.getNotificationTS(env, "bob", BALANCE);

    Assert.assertTrue(notTS1 < notTS2);

    // even though there were two notifications and TX is using 1st notification TS... only 1st TX
    // should execute
    // google paper calls this message collapsing

    TestTransaction tx3 = new TestTransaction(env, "bob", BALANCE, notTS1);

    TestTransaction tx2 = new TestTransaction(env, "bob", BALANCE, notTS1);
    Assert.assertEquals("11", tx2.gets("bob", BALANCE));
    tx2.done();

    Assert.assertEquals("11", tx3.gets("bob", BALANCE));
    assertAAck(tx3);

    TestTransaction tx4 = new TestTransaction(env, "bob", BALANCE, notTS2);
    Assert.assertEquals("11", tx4.gets("bob", BALANCE));
    assertAAck(tx4);
  }

  @Test
  public void testWriteObserved() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged
    // status

    TestTransaction tx = new TestTransaction(env);

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");

    tx.done();

    TestTransaction tx2 = new TestTransaction(env, "joe", BALANCE);
    tx2.gets("joe", BALANCE);
    tx2.set("joe", BALANCE, "21");
    tx2.set("bob", BALANCE, "11");

    TestTransaction tx1 = new TestTransaction(env, "joe", BALANCE);
    tx1.gets("joe", BALANCE);
    tx1.set("jill", BALANCE, "61");

    tx1.done();
    assertAAck(tx2);

    TestTransaction tx3 = new TestTransaction(env);

    Assert.assertEquals("10", tx3.gets("bob", BALANCE));
    Assert.assertEquals("20", tx3.gets("joe", BALANCE));
    Assert.assertEquals("61", tx3.gets("jill", BALANCE));

    tx3.done();
  }

  @Test
  public void testVisibility() throws Exception {

    conn.securityOperations().changeUserAuthorizations(USER, new Authorizations("A", "B", "C"));

    env.setAuthorizations(new Authorizations("A", "B", "C"));

    Column balanceCol = new Column("account", "balance", "A|B");

    TestTransaction tx = new TestTransaction(env);

    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");

    tx.done();

    FluoConfiguration fc = new FluoConfiguration(config);
    Environment env2 = new Environment(fc);
    env2.setAuthorizations(new Authorizations("B"));

    TestTransaction tx2 = new TestTransaction(env2);
    Assert.assertEquals("10", tx2.gets("bob", balanceCol));
    Assert.assertEquals("20", tx2.gets("joe", balanceCol));
    Assert.assertEquals("60", tx2.gets("jill", balanceCol));
    tx2.done();
    env2.close();

    Environment env3 = new Environment(fc);
    env3.setAuthorizations(new Authorizations("C"));

    TestTransaction tx3 = new TestTransaction(env3);
    Assert.assertNull(tx3.gets("bob", balanceCol));
    Assert.assertNull(tx3.gets("joe", balanceCol));
    Assert.assertNull(tx3.gets("jill", balanceCol));
    tx3.done();
    env3.close();
  }

  @Test
  public void testRange() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged
    // status

    TestTransaction tx = new TestTransaction(env);
    tx.set("d00001", new Column("data", "content"),
        "blah blah, blah http://a.com. Blah blah http://b.com.  Blah http://c.com");
    tx.set("d00001", new Column("outlink", "http://a.com"), "");
    tx.set("d00001", new Column("outlink", "http://b.com"), "");
    tx.set("d00001", new Column("outlink", "http://c.com"), "");

    tx.set("d00002", new Column("data", "content"),
        "blah blah, blah http://d.com. Blah blah http://e.com.  Blah http://c.com");
    tx.set("d00002", new Column("outlink", "http://d.com"), "");
    tx.set("d00002", new Column("outlink", "http://e.com"), "");
    tx.set("d00002", new Column("outlink", "http://c.com"), "");

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);

    TestTransaction tx3 = new TestTransaction(env);

    tx3.set("d00001", new Column("data", "content"),
        "blah blah, blah http://a.com. Blah http://c.com .  Blah http://z.com");
    tx3.set("d00001", new Column("outlink", "http://a.com"), "");
    tx3.delete("d00001", new Column("outlink", "http://b.com"));
    tx3.set("d00001", new Column("outlink", "http://c.com"), "");
    tx3.set("d00001", new Column("outlink", "http://z.com"), "");

    tx3.done();

    HashSet<Column> columns = new HashSet<>();

    CellScanner cellScanner =
        tx2.scanner().over(Span.exact(Bytes.of("d00001"))).fetch(new Column("outlink")).build();
    for (RowColumnValue rcv : cellScanner) {
      columns.add(rcv.getColumn());
    }

    tx2.done();

    HashSet<Column> expected = new HashSet<>();
    expected.add(new Column("outlink", "http://a.com"));
    expected.add(new Column("outlink", "http://b.com"));
    expected.add(new Column("outlink", "http://c.com"));

    Assert.assertEquals(expected, columns);

    TestTransaction tx4 = new TestTransaction(env);
    columns.clear();
    cellScanner =
        tx4.scanner().over(Span.exact(Bytes.of("d00001"))).fetch(new Column("outlink")).build();
    for (RowColumnValue rcv : cellScanner) {
      columns.add(rcv.getColumn());
    }

    expected.add(new Column("outlink", "http://z.com"));
    expected.remove(new Column("outlink", "http://b.com"));
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
    Map<String, ImmutableMap<Column, String>> expected1 = ImmutableMap.of("d:0001",
        ImmutableMap.of(ccol, "abc def"), "d:0002", ImmutableMap.of(ccol, "neb feg"));
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
