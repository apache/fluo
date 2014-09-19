/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.core.impl;

import java.util.HashSet;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.exceptions.CommitException;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.core.TestBaseImpl;
import io.fluo.core.TestTransaction;
import io.fluo.core.exceptions.AlreadyAcknowledgedException;
import io.fluo.core.impl.TransactionImpl.CommitData;
import io.fluo.core.oracle.OracleClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

public class FluoIT extends TestBaseImpl {
  
  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());
  
  @Test
  public void testFluoFactory() throws Exception {
    Assert.assertNotNull(FluoFactory.newAdmin(config));

    try (FluoClient client = FluoFactory.newClient(config)) {
      Assert.assertNotNull(client);
      Assert.assertNotNull(client.newLoaderExecutor());

      try (Snapshot s = client.newSnapshot()) {
        Assert.assertNotNull(s);
        s.get(Bytes.wrap("test"), new Column(Bytes.wrap("cf"), Bytes.wrap("cq")));
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
    
    tx.mutate().row("bob").col(balanceCol).set("10");
    tx.mutate().row("joe").col(balanceCol).set("20");
    tx.mutate().row("jill").col(balanceCol).set("60");
    
    tx.done();
    
    tx = new TestTransaction(env);
    
    String bal1 = tx.get().row("bob").col(balanceCol).toString();
    String bal2 = tx.get().row("joe").col(balanceCol).toString();
    Assert.assertEquals("10", bal1);
    Assert.assertEquals("20", bal2);
    
    tx.mutate().row("bob").col(balanceCol).set((Long.parseLong(bal1) - 5) + "");
    tx.mutate().row("joe").col(balanceCol).set((Long.parseLong(bal2) + 5) + "");
    
    TestTransaction tx2 = new TestTransaction(env);
    
    String bal3 = tx2.get().row("bob").col(balanceCol).toString();
    String bal4 = tx2.get().row("jill").col(balanceCol).toString();
    Assert.assertEquals("10", bal3);
    Assert.assertEquals("60", bal4);
    
    tx2.mutate().row("bob").col(balanceCol).set((Long.parseLong(bal3) - 5) + "");
    tx2.mutate().row("jill").col(balanceCol).set((Long.parseLong(bal4) + 5) + "");
    
    tx2.done();
    assertCommitFails(tx);
    
    TestTransaction tx3 = new TestTransaction(env);
    
    Assert.assertEquals("5", tx3.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("20", tx3.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("65", tx3.get().row("jill").col(balanceCol).toString());


  }
  
  private void assertCommitFails(TestTransaction tx) {
    try {
      tx.commit();
      Assert.fail();
    } catch (CommitException ce) {
      
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
    tx2.mutate().row("jill").col(balanceCol).set(tx2.get().row("jill").col(balanceCol).toLong() + 10);

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
    
    TestTransaction tx5 = new TestTransaction(env);
    
    Assert.assertEquals(5l, tx5.get().row("bob").col(balanceCol).toLong(0));
    Assert.assertEquals(15l, tx5.get().row("joe").col(balanceCol).toLong(0));
    Assert.assertEquals(70l, tx5.get().row("jill").col(balanceCol).toLong(0));
    Assert.assertEquals(3l, tx5.get().row("jane").col(balanceCol).toLong(0));
  }
  
  @Test
  public void testAck() throws Exception {
    // when two transactions run against the same observed column, only one should commit
    
    TestTransaction tx = new TestTransaction(env);
    
    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis();
    
    tx.mutate().row("bob").col(balanceCol).set("10");
    tx.mutate().row("joe").col(balanceCol).set("20");
    tx.mutate().row("jill").col(balanceCol).set("60");
    
    tx.done();
    
    TestTransaction tx1 = new TestTransaction(env, "joe", balanceCol);
    tx1.get().row("joe").col(balanceCol);
    tx1.mutate().row("jill").col(balanceCol).set("61");
    
    TestTransaction tx2 = new TestTransaction(env, "joe", balanceCol);
    tx2.get().row("joe").col(balanceCol);
    tx2.mutate().row("bob").col(balanceCol).set("11");
    
    tx1.done();
    assertCommitFails(tx2);
    
    TestTransaction tx3 = new TestTransaction(env);
    
    Assert.assertEquals("10", tx3.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("20", tx3.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("61", tx3.get().row("jill").col(balanceCol).toString());
    
    // update joe, so it can be acknowledged again
    tx3.mutate().row("joe").col(balanceCol).set("21");
    
    tx3.done();
    
    TestTransaction tx4 = new TestTransaction(env, "joe", balanceCol);
    tx4.get().row("joe").col(balanceCol);
    tx4.mutate().row("jill").col(balanceCol).set("62");
    
    TestTransaction tx5 = new TestTransaction(env, "joe", balanceCol);
    tx5.get().row("joe").col(balanceCol);
    tx5.mutate().row("bob").col(balanceCol).set("11");
    
    // make the 2nd transaction to start commit 1st
    tx5.done();
    assertCommitFails(tx4);
    
    TestTransaction tx6 = new TestTransaction(env);
    
    Assert.assertEquals("11", tx6.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("21", tx6.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("61", tx6.get().row("jill").col(balanceCol).toString());
    
    TestTransaction tx7 = new TestTransaction(env, "joe", balanceCol);
    tx7.get().row("joe").col(balanceCol);
    tx7.mutate().row("bob").col(balanceCol).set("15");
    tx7.mutate().row("jill").col(balanceCol).set("60");
    
    try {
      tx7.commit();
      Assert.fail();
    } catch (AlreadyAcknowledgedException aae) {}
    
    TestTransaction tx8 = new TestTransaction(env);
    
    Assert.assertEquals("11", tx8.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("21", tx8.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("61", tx8.get().row("jill").col(balanceCol).toString());
  }
  
  @Test
  public void testAck2() throws Exception {
    TestTransaction tx = new TestTransaction(env);

    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis();
    Column addrCol = typeLayer.bc().fam("account").qual("addr").vis();

    tx.mutate().row("bob").col(balanceCol).set("10");
    tx.mutate().row("joe").col(balanceCol).set("20");
    tx.mutate().row("jill").col(balanceCol).set("60");

    tx.done();

    TestTransaction tx1 = new TestTransaction(env, "bob", balanceCol);
    TestTransaction tx2 = new TestTransaction(env, "bob", balanceCol);

    tx1.get().row("bob").col(balanceCol).toString();
    tx2.get().row("bob").col(balanceCol).toString();

    tx1.get().row("bob").col(addrCol).toString();
    tx2.get().row("bob").col(addrCol).toString();

    tx1.mutate().row("bob").col(addrCol).set("1 loop pl");
    tx2.mutate().row("bob").col(addrCol).set("1 loop pl");

    // this test overlaps the commmits of two transactions w/ the same trigger

    CommitData cd = tx1.createCommitData();
    Assert.assertTrue(tx1.preCommit(cd));

    try {
      tx2.commit();
    } catch (CommitException ce) {

    }

    long commitTs = OracleClient.getInstance(env).getTimestamp();
    Assert.assertTrue(tx1.commitPrimaryColumn(cd, commitTs));
    tx1.finishCommit(cd, commitTs);

    TestTransaction tx3 = new TestTransaction(env, "bob", balanceCol);
    tx3.mutate().row("bob").col(addrCol).set("2 loop pl");
    try {
      tx3.commit();
    } catch (AlreadyAcknowledgedException e) {

    }

  }

  @Test
  public void testWriteObserved() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged status
    
    TestTransaction tx = new TestTransaction(env);
    
    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis();
    
    tx.mutate().row("bob").col(balanceCol).set("10");
    tx.mutate().row("joe").col(balanceCol).set("20");
    tx.mutate().row("jill").col(balanceCol).set("60");
    
    tx.done();
    
    TestTransaction tx2 = new TestTransaction(env, "joe", balanceCol);
    tx2.get().row("joe").col(balanceCol);
    tx2.mutate().row("joe").col(balanceCol).set("21");
    tx2.mutate().row("bob").col(balanceCol).set("11");
    
    TestTransaction tx1 = new TestTransaction(env, "joe", balanceCol);
    tx1.get().row("joe").col(balanceCol);
    tx1.mutate().row("jill").col(balanceCol).set("61");
    
    tx1.done();
    assertCommitFails(tx2);
    
    TestTransaction tx3 = new TestTransaction(env);
    
    Assert.assertEquals("10", tx3.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("20", tx3.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("61", tx3.get().row("jill").col(balanceCol).toString());
    
  }
  
  @Test
  public void testVisibility() throws Exception {

    conn.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C"));

    env.setAuthorizations(new Authorizations("A", "B", "C"));
    
    Column balanceCol = typeLayer.bc().fam("account").qual("balance").vis();
    balanceCol.setVisibility("A|B");

    TestTransaction tx = new TestTransaction(env);
    
    tx.mutate().row("bob").col(balanceCol).set("10");
    tx.mutate().row("joe").col(balanceCol).set("20");
    tx.mutate().row("jill").col(balanceCol).set("60");
    
    tx.done();
    
    Environment env2 = new Environment(curator, zkn, conn, FluoConfiguration.ORACLE_PORT_DEFAULT);
    env2.setAuthorizations(new Authorizations("B"));
    
    TestTransaction tx2 = new TestTransaction(env2);
    Assert.assertEquals("10", tx2.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("20", tx2.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("60", tx2.get().row("jill").col(balanceCol).toString());
    env2.close();
    
    Environment env3 = new Environment(curator, zkn, conn, FluoConfiguration.ORACLE_PORT_DEFAULT);
    env3.setAuthorizations(new Authorizations("C"));
    
    TestTransaction tx3 = new TestTransaction(env3);
    Assert.assertNull(tx3.get().row("bob").col(balanceCol).toString());
    Assert.assertNull(tx3.get().row("joe").col(balanceCol).toString());
    Assert.assertNull(tx3.get().row("jill").col(balanceCol).toString());
    env3.close();
  }

  @Test
  public void testRange() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged status
    
    TestTransaction tx = new TestTransaction(env);
    tx.mutate().row("d00001").col(typeLayer.bc().fam("data").qual("content").vis()).set("blah blah, blah http://a.com. Blah blah http://b.com.  Blah http://c.com");
    tx.mutate().row("d00001").col(typeLayer.bc().fam("outlink").qual("http://a.com").vis()).set("");
    tx.mutate().row("d00001").col(typeLayer.bc().fam("outlink").qual("http://b.com").vis()).set("");
    tx.mutate().row("d00001").col(typeLayer.bc().fam("outlink").qual("http://c.com").vis()).set("");
    
    tx.mutate().row("d00002").col(typeLayer.bc().fam("data").qual("content").vis()).set("blah blah, blah http://d.com. Blah blah http://e.com.  Blah http://c.com");
    tx.mutate().row("d00002").col(typeLayer.bc().fam("outlink").qual("http://d.com").vis()).set("");
    tx.mutate().row("d00002").col(typeLayer.bc().fam("outlink").qual("http://e.com").vis()).set("");
    tx.mutate().row("d00002").col(typeLayer.bc().fam("outlink").qual("http://c.com").vis()).set("");
    
    tx.done();
    
    TestTransaction tx2 = new TestTransaction(env);
    
    TestTransaction tx3 = new TestTransaction(env);
    
    tx3.mutate().row("d00001").col(typeLayer.bc().fam("data").qual("content").vis()).set("blah blah, blah http://a.com. Blah http://c.com .  Blah http://z.com");
    tx3.mutate().row("d00001").col(typeLayer.bc().fam("outlink").qual("http://a.com").vis()).set("");
    tx3.mutate().row("d00001").col(typeLayer.bc().fam("outlink").qual("http://b.com").vis()).delete();
    tx3.mutate().row("d00001").col(typeLayer.bc().fam("outlink").qual("http://c.com").vis()).set("");
    tx3.mutate().row("d00001").col(typeLayer.bc().fam("outlink").qual("http://z.com").vis()).set("");
    
    tx3.done();
    
    HashSet<Column> columns = new HashSet<>();
    RowIterator riter = tx2.get(new ScannerConfiguration().setSpan(Span.exact(Bytes.wrap("d00001"), Bytes.wrap("outlink"))));
    while (riter.hasNext()) {
      ColumnIterator citer = riter.next().getValue();
      while (citer.hasNext()) {
        columns.add(citer.next().getKey());
      }
    }
    
    HashSet<Column> expected = new HashSet<>();
    expected.add(typeLayer.bc().fam("outlink").qual("http://a.com").vis());
    expected.add(typeLayer.bc().fam("outlink").qual("http://b.com").vis());
    expected.add(typeLayer.bc().fam("outlink").qual("http://c.com").vis());
    
    Assert.assertEquals(expected, columns);
    
    TestTransaction tx4 = new TestTransaction(env);
    columns.clear();
    riter = tx4.get(new ScannerConfiguration().setSpan(Span.exact(Bytes.wrap("d00001"), Bytes.wrap("outlink"))));
    while (riter.hasNext()) {
      ColumnIterator citer = riter.next().getValue();
      while (citer.hasNext()) {
        columns.add(citer.next().getKey());
      }
    }
    expected.add(typeLayer.bc().fam("outlink").qual("http://z.com").vis());
    expected.remove(typeLayer.bc().fam("outlink").qual("http://b.com").vis());
    Assert.assertEquals(expected, columns);
    
  }
}
