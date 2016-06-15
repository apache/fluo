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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.LongUtil;
import org.apache.fluo.accumulo.util.ZookeeperUtil;
import org.apache.fluo.accumulo.values.DelLockValue;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.ObserverConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;
import org.apache.fluo.core.exceptions.AlreadyAcknowledgedException;
import org.apache.fluo.core.exceptions.StaleScanException;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.impl.TransactionImpl;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.impl.TransactorNode;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.integration.BankUtil;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.fluo.integration.BankUtil.BALANCE;

public class FailureIT extends ITBaseImpl {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  public static class NullObserver extends AbstractObserver {

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {}

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(typeLayer.bc().fam("attr").qual("lastupdate").vis(),
          NotificationType.STRONG);
    }
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    List<ObserverConfiguration> observed = new ArrayList<>();
    observed.add(new ObserverConfiguration(NullObserver.class.getName()));
    return observed;
  }

  @Test
  public void testRollbackMany() throws Exception {
    testRollbackMany(true);
  }

  @Test
  public void testRollbackManyTimeout() throws Exception {
    testRollbackMany(false);
  }

  public void testRollbackMany(boolean killTransactor) throws Exception {

    // test writing lots of columns that need to be rolled back

    Column col1 = typeLayer.bc().fam("fam1").qual("q1").vis();
    Column col2 = typeLayer.bc().fam("fam1").qual("q2").vis();

    TestTransaction tx = new TestTransaction(env);

    for (int r = 0; r < 10; r++) {
      tx.mutate().row(r + "").col(col1).set("0" + r + "0");
      tx.mutate().row(r + "").col(col2).set("0" + r + "1");
    }

    tx.done();

    TransactorNode t2 = new TransactorNode(env);
    TestTransaction tx2 = new TestTransaction(env, t2);

    for (int r = 0; r < 10; r++) {
      tx2.mutate().row(r + "").col(col1).set("1" + r + "0");
      tx2.mutate().row(r + "").col(col2).set("1" + r + "1");
    }

    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));

    if (killTransactor) {
      t2.close();
    }

    TestTransaction tx3 = new TestTransaction(env);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("0" + r + "0", tx3.get().row(r + "").col(col1).toString());
      Assert.assertEquals("0" + r + "1", tx3.get().row(r + "").col(col2).toString());
    }

    if (killTransactor) {
      Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
      exception.expect(FluoException.class);
      tx2.commitPrimaryColumn(cd, commitTs);
    } else {
      Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
      Assert.assertFalse(tx2.commitPrimaryColumn(cd, commitTs));
      t2.close();
    }

    TestTransaction tx4 = new TestTransaction(env);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("0" + r + "0", tx4.get().row(r + "").col(col1).toString());
      Assert.assertEquals("0" + r + "1", tx4.get().row(r + "").col(col2).toString());
    }
  }

  @Test
  public void testRollforwardMany() throws Exception {
    testRollforwardMany(true);
  }

  @Test
  public void testRollforwardManyTimeout() throws Exception {
    testRollforwardMany(false);
  }

  public void testRollforwardMany(boolean killTransactor) throws Exception {
    // test writing lots of columns that need to be rolled forward

    Column col1 = typeLayer.bc().fam("fam1").qual("q1").vis();
    Column col2 = typeLayer.bc().fam("fam1").qual("q2").vis();

    TestTransaction tx = new TestTransaction(env);

    for (int r = 0; r < 10; r++) {
      tx.mutate().row(r + "").col(col1).set("0" + r + "0");
      tx.mutate().row(r + "").col(col2).set("0" + r + "1");
    }

    tx.done();

    TransactorNode t2 = new TransactorNode(env);
    TestTransaction tx2 = new TestTransaction(env, t2);

    for (int r = 0; r < 10; r++) {
      tx2.mutate().row(r + "").col(col1).set("1" + r + "0");
      tx2.mutate().row(r + "").col(col2).set("1" + r + "1");
    }

    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));
    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(tx2.commitPrimaryColumn(cd, commitTs));

    if (killTransactor) {
      t2.close();
    }

    TestTransaction tx3 = new TestTransaction(env);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("1" + r + "0", tx3.get().row(r + "").col(col1).toString());
      Assert.assertEquals("1" + r + "1", tx3.get().row(r + "").col(col2).toString());
    }

    tx2.finishCommit(cd, commitTs);

    TestTransaction tx4 = new TestTransaction(env);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("1" + r + "0", tx4.get().row(r + "").col(col1).toString());
      Assert.assertEquals("1" + r + "1", tx4.get().row(r + "").col(col2).toString());
    }

    if (!killTransactor) {
      t2.close();
    }
  }

  @Test
  public void testRollback() throws Exception {
    // test the case where a scan encounters a stuck lock and rolls it back

    TestTransaction tx = new TestTransaction(env);

    tx.mutate().row("bob").col(BALANCE).set(10);
    tx.mutate().row("joe").col(BALANCE).set(20);
    tx.mutate().row("jill").col(BALANCE).set(60);

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);

    int bal1 = tx2.get().row("bob").col(BALANCE).toInteger(0);
    int bal2 = tx2.get().row("joe").col(BALANCE).toInteger(0);

    tx2.mutate().row("bob").col(BALANCE).set(bal1 - 7);
    tx2.mutate().row("joe").col(BALANCE).set(bal2 + 7);

    // get locks
    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));

    // test rolling back primary and non-primary columns

    int bobBal = 10;
    int joeBal = 20;
    if ((new Random()).nextBoolean()) {
      BankUtil.transfer(env, "joe", "jill", 7);
      joeBal -= 7;
    } else {
      BankUtil.transfer(env, "bob", "jill", 7);
      bobBal -= 7;
    }

    TestTransaction tx4 = new TestTransaction(env);

    Assert.assertEquals(bobBal, tx4.get().row("bob").col(BALANCE).toInteger(0));
    Assert.assertEquals(joeBal, tx4.get().row("joe").col(BALANCE).toInteger(0));
    Assert.assertEquals(67, tx4.get().row("jill").col(BALANCE).toInteger(0));

    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertFalse(tx2.commitPrimaryColumn(cd, commitTs));

    BankUtil.transfer(env, "bob", "joe", 2);
    bobBal -= 2;
    joeBal += 2;

    TestTransaction tx6 = new TestTransaction(env);

    Assert.assertEquals(bobBal, tx6.get().row("bob").col(BALANCE).toInteger(0));
    Assert.assertEquals(joeBal, tx6.get().row("joe").col(BALANCE).toInteger(0));
    Assert.assertEquals(67, tx6.get().row("jill").col(BALANCE).toInteger(0));
  }

  @Test
  public void testDeadRollback() throws Exception {
    rollbackTest(true);
  }

  @Test
  public void testTimeoutRollback() throws Exception {
    rollbackTest(false);
  }

  private void rollbackTest(boolean killTransactor) throws Exception {
    TransactorNode t1 = new TransactorNode(env);

    TestTransaction tx = new TestTransaction(env);

    tx.mutate().row("bob").col(BALANCE).set(10);
    tx.mutate().row("joe").col(BALANCE).set(20);
    tx.mutate().row("jill").col(BALANCE).set(60);

    tx.done();

    TestTransaction tx2 = new TestTransaction(env, t1);

    int bal1 = tx2.get().row("bob").col(BALANCE).toInteger(0);
    int bal2 = tx2.get().row("joe").col(BALANCE).toInteger(0);

    tx2.mutate().row("bob").col(BALANCE).set(bal1 - 7);
    tx2.mutate().row("joe").col(BALANCE).set(bal2 + 7);

    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));

    if (killTransactor) {
      t1.close();
    }

    TransactionImpl tx3 = new TransactionImpl(env);
    Assert.assertEquals(0, tx3.getStats().getDeadLocks());
    Assert.assertEquals(0, tx3.getStats().getTimedOutLocks());

    int bobFinal = Integer.parseInt(tx3.get(Bytes.of("bob"), BALANCE).toString());
    Assert.assertEquals(10, bobFinal);

    long startTs = tx2.getStartTimestamp();
    // one and only one of the rolled back locks should be marked as primary
    Assert.assertTrue(wasRolledBackPrimary(startTs, "bob") ^ wasRolledBackPrimary(startTs, "joe"));

    if (killTransactor) {
      Assert.assertEquals(1, tx3.getStats().getDeadLocks());
      Assert.assertEquals(0, tx3.getStats().getTimedOutLocks());
    } else {
      Assert.assertEquals(0, tx3.getStats().getDeadLocks());
      Assert.assertEquals(1, tx3.getStats().getTimedOutLocks());
    }

    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();

    if (killTransactor) {
      // test for exception
      exception.expect(FluoException.class);
      tx2.commitPrimaryColumn(cd, commitTs);
    } else {
      Assert.assertFalse(tx2.commitPrimaryColumn(cd, commitTs));
      t1.close();
    }
    tx3.close();
  }

  @Test
  public void testRollfoward() throws Exception {
    // test the case where a scan encounters a stuck lock (for a complete tx) and rolls it forward

    TestTransaction tx = new TestTransaction(env);

    tx.mutate().row("bob").col(BALANCE).set(10);
    tx.mutate().row("joe").col(BALANCE).set(20);
    tx.mutate().row("jill").col(BALANCE).set(60);

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);

    int bal1 = tx2.get().row("bob").col(BALANCE).toInteger(0);
    int bal2 = tx2.get().row("joe").col(BALANCE).toInteger(0);

    tx2.mutate().row("bob").col(BALANCE).set(bal1 - 7);
    tx2.mutate().row("joe").col(BALANCE).set(bal2 + 7);

    // get locks
    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));
    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(tx2.commitPrimaryColumn(cd, commitTs));

    // test rolling forward primary and non-primary columns
    int bobBal = 3;
    int joeBal = 27;
    if ((new Random()).nextBoolean()) {
      BankUtil.transfer(env, "joe", "jill", 2);
      joeBal = 25;
    } else {
      BankUtil.transfer(env, "bob", "jill", 2);
      bobBal = 1;
    }

    TestTransaction tx4 = new TestTransaction(env);

    Assert.assertEquals(bobBal, tx4.get().row("bob").col(BALANCE).toInteger(0));
    Assert.assertEquals(joeBal, tx4.get().row("joe").col(BALANCE).toInteger(0));
    Assert.assertEquals(62, tx4.get().row("jill").col(BALANCE).toInteger(0));

    tx2.finishCommit(cd, commitTs);

    TestTransaction tx5 = new TestTransaction(env);

    Assert.assertEquals(bobBal, tx5.get().row("bob").col(BALANCE).toInteger(0));
    Assert.assertEquals(joeBal, tx5.get().row("joe").col(BALANCE).toInteger(0));
    Assert.assertEquals(62, tx5.get().row("jill").col(BALANCE).toInteger(0));
  }

  @Test
  public void testAcks() throws Exception {
    // TODO test that acks are properly handled in rollback and rollforward

    TestTransaction tx = new TestTransaction(env);

    tx.mutate().row("url0000").fam("attr").qual("lastupdate").set(3);
    tx.mutate().row("url0000").fam("doc").qual("content").set("abc def");

    tx.done();

    TestTransaction tx2 =
        new TestTransaction(env, "url0000", typeLayer.bc().fam("attr").qual("lastupdate").vis());
    tx2.mutate().row("idx:abc").fam("doc").qual("url").set("url0000");
    tx2.mutate().row("idx:def").fam("doc").qual("url").set("url0000");
    CommitData cd = tx2.createCommitData();
    tx2.preCommit(cd);

    TestTransaction tx3 = new TestTransaction(env);
    Assert.assertNull(tx3.get().row("idx:abc").fam("doc").qual("url").toString());
    Assert.assertNull(tx3.get().row("idx:def").fam("doc").qual("url").toString());
    Assert.assertEquals(3, tx3.get().row("url0000").fam("attr").qual("lastupdate").toInteger(0));

    Scanner scanner = env.getConnector().createScanner(env.getTable(), Authorizations.EMPTY);
    Notification.configureScanner(scanner);
    Iterator<Entry<Key, Value>> iter = scanner.iterator();
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals("url0000", iter.next().getKey().getRow().toString());

    TestTransaction tx5 =
        new TestTransaction(env, "url0000", typeLayer.bc().fam("attr").qual("lastupdate").vis());
    tx5.mutate().row("idx:abc").fam("doc").qual("url").set("url0000");
    tx5.mutate().row("idx:def").fam("doc").qual("url").set("url0000");
    cd = tx5.createCommitData();
    Assert.assertTrue(tx5.preCommit(cd));
    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(tx5.commitPrimaryColumn(cd, commitTs));

    // should roll tx5 forward
    TestTransaction tx6 = new TestTransaction(env);
    Assert.assertEquals(3, tx6.get().row("url0000").fam("attr").qual("lastupdate").toInteger(0));
    Assert.assertEquals("url0000", tx6.get().row("idx:abc").fam("doc").qual("url").toString());
    Assert.assertEquals("url0000", tx6.get().row("idx:def").fam("doc").qual("url").toString());

    iter = scanner.iterator();
    Assert.assertTrue(iter.hasNext());

    // TODO is tx4 start before tx5, then this test will not work because AlreadyAck is not thrown
    // for overlapping.. CommitException is thrown
    TestTransaction tx4 =
        new TestTransaction(env, "url0000", typeLayer.bc().fam("attr").qual("lastupdate").vis());
    tx4.mutate().row("idx:abc").fam("doc").qual("url").set("url0000");
    tx4.mutate().row("idx:def").fam("doc").qual("url").set("url0000");

    try {
      // should not go through if tx5 is properly rolled forward
      tx4.commit();
      Assert.fail();
    } catch (AlreadyAcknowledgedException aae) {
    }

    // commit above should schedule async delete of notification
    env.getSharedResources().getBatchWriter().waitForAsyncFlush();
    iter = scanner.iterator();
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testStaleScanPrevention() throws Exception {

    TestTransaction tx = new TestTransaction(env);

    tx.mutate().row("bob").col(BALANCE).set(10);
    tx.mutate().row("joe").col(BALANCE).set(20);
    tx.mutate().row("jill").col(BALANCE).set(60);

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);
    Assert.assertEquals(10, tx2.get().row("bob").col(BALANCE).toInteger(0));

    BankUtil.transfer(env, "joe", "jill", 1);
    BankUtil.transfer(env, "joe", "bob", 1);
    BankUtil.transfer(env, "bob", "joe", 2);
    BankUtil.transfer(env, "jill", "joe", 2);

    conn.tableOperations().flush(table, null, null, true);

    Assert.assertEquals(20, tx2.get().row("joe").col(BALANCE).toInteger(0));

    // Stale scan should not occur due to oldest active timestamp tracking in Zookeeper
    tx2.close();

    TestTransaction tx3 = new TestTransaction(env);

    Assert.assertEquals(9, tx3.get().row("bob").col(BALANCE).toInteger(0));
    Assert.assertEquals(22, tx3.get().row("joe").col(BALANCE).toInteger(0));
    Assert.assertEquals(59, tx3.get().row("jill").col(BALANCE).toInteger(0));
  }

  @Test(timeout = 60000)
  public void testForcedStaleScan() throws Exception {

    TestTransaction tx = new TestTransaction(env);

    tx.mutate().row("bob").col(BALANCE).set(10);
    tx.mutate().row("joe").col(BALANCE).set(20);
    tx.mutate().row("jill").col(BALANCE).set(60);
    tx.mutate().row("john").col(BALANCE).set(3);

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);
    Assert.assertEquals(10, tx2.get().row("bob").col(BALANCE).toInteger(0));

    TestTransaction tx3 = new TestTransaction(env);
    tx3.get().row("john").col(BALANCE).toInteger(0);

    BankUtil.transfer(env, "joe", "jill", 1);
    BankUtil.transfer(env, "joe", "bob", 1);
    BankUtil.transfer(env, "bob", "joe", 2);
    BankUtil.transfer(env, "jill", "joe", 2);

    // Force a stale scan be modifying the oldest active timestamp to a
    // more recent time in Zookeeper. This disables timestamp tracking.
    Long nextTs = new TestTransaction(env).getStartTs();
    CuratorFramework curator = env.getSharedResources().getCurator();
    curator.setData().forPath(env.getSharedResources().getTimestampTracker().getNodePath(),
        LongUtil.toByteArray(nextTs));

    long gcTs = ZookeeperUtil.getGcTimestamp(config.getAppZookeepers());
    while (gcTs < nextTs) {
      Thread.sleep(500);
      // keep setting timestamp tracker time in ZK until GC picks it up
      curator.setData().forPath(env.getSharedResources().getTimestampTracker().getNodePath(),
          LongUtil.toByteArray(nextTs));
      gcTs = ZookeeperUtil.getGcTimestamp(config.getAppZookeepers());
    }

    // GC iterator will clear data that tx2 wants to scan
    conn.tableOperations().flush(table, null, null, true);

    // this data should have been GCed, but the problem is not detected here
    Assert.assertNull(tx2.get().row("joe").col(BALANCE).toInteger());

    try {
      // closing should detect the stale scan
      tx2.close();
      Assert.assertFalse(true);
    } catch (StaleScanException sse) {

    }

    tx3.mutate().row("john").col(BALANCE).set(5l);

    try {
      tx3.commit();
      Assert.assertFalse(true);
    } catch (CommitException e) {
      // should not throw an exception
      tx3.close();
    }

    TestTransaction tx4 = new TestTransaction(env);

    Assert.assertEquals(9, tx4.get().row("bob").col(BALANCE).toInteger(0));
    Assert.assertEquals(22, tx4.get().row("joe").col(BALANCE).toInteger(0));
    Assert.assertEquals(59, tx4.get().row("jill").col(BALANCE).toInteger(0));
    Assert.assertEquals(3, tx4.get().row("john").col(BALANCE).toInteger(0));
  }

  @Test
  public void testCommitBug1() throws Exception {

    TestTransaction tx1 = new TestTransaction(env);

    tx1.mutate().row("bob").col(BALANCE).set(10);
    tx1.mutate().row("joe").col(BALANCE).set(20);
    tx1.mutate().row("jill").col(BALANCE).set(60);

    CommitData cd = tx1.createCommitData();
    Assert.assertTrue(tx1.preCommit(cd));

    while (true) {
      TestTransaction tx2 = new TestTransaction(env);

      tx2.mutate().row("bob").col(BALANCE).set(11);
      tx2.mutate().row("jill").col(BALANCE).set(61);

      // tx1 should be rolled back even in case where columns tx1 locked are not read by tx2
      try {
        tx2.commit();
        break;
      } catch (CommitException ce) {

      }
    }

    TestTransaction tx4 = new TestTransaction(env);

    Assert.assertEquals(11, tx4.get().row("bob").col(BALANCE).toInteger(0));
    Assert.assertNull(tx4.get().row("joe").col(BALANCE).toInteger());
    Assert.assertEquals(61, tx4.get().row("jill").col(BALANCE).toInteger(0));
  }

  @Test
  public void testRollbackSelf() throws Exception {
    // test for #660... ensure when transaction rolls itself back that it properly sets the primary
    // flag

    TestTransaction tx1 = new TestTransaction(env);

    tx1.mutate().row("bob").col(BALANCE).set(10);
    tx1.mutate().row("joe").col(BALANCE).set(20);
    tx1.mutate().row("jill").col(BALANCE).set(60);

    tx1.done();


    TestTransaction tx2 = new TestTransaction(env, "jill", BALANCE, 1);

    TestTransaction tx3 = new TestTransaction(env);
    tx3.mutate().row("bob").col(BALANCE).increment(5);
    tx3.mutate().row("joe").col(BALANCE).increment(-5);
    tx3.done();

    tx2.mutate().row("bob").col(BALANCE).increment(5);
    tx2.mutate().row("jill").col(BALANCE).increment(-5);


    // should be able to successfully lock the primary column jill... but then should fail to lock
    // bob and have to rollback
    try {
      tx2.commit();
      Assert.fail("Expected commit exception");
    } catch (CommitException ce) {

    }


    boolean sawExpected = wasRolledBackPrimary(tx2.getStartTimestamp(), "jill");

    Assert.assertTrue(sawExpected);

    TestTransaction tx4 = new TestTransaction(env);
    Assert.assertEquals("15", tx4.gets("bob", BALANCE));
    Assert.assertEquals("15", tx4.gets("joe", BALANCE));
    Assert.assertEquals("60", tx4.gets("jill", BALANCE));
    tx4.close();

  }

  private boolean wasRolledBackPrimary(long startTs, String rolledBackRow)
      throws TableNotFoundException {
    boolean sawExpected = false;
    Scanner scanner = conn.createScanner(getCurTableName(), Authorizations.EMPTY);

    for (Entry<Key, Value> entry : scanner) {
      long colType = entry.getKey().getTimestamp() & ColumnConstants.PREFIX_MASK;
      long ts = entry.getKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;
      String row = entry.getKey().getRowData().toString();
      byte[] val = entry.getValue().get();

      if (row.equals(rolledBackRow) && colType == ColumnConstants.DEL_LOCK_PREFIX && ts == startTs
          && DelLockValue.isPrimary(val)) {
        sawExpected = true;
      }
    }
    return sawExpected;
  }

}
