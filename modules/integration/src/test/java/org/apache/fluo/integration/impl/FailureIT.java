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

import java.util.Iterator;
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
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.ObserverProvider;
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
import org.apache.fluo.integration.TestUtil;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.fluo.api.observer.Observer.NotificationType.STRONG;
import static org.apache.fluo.integration.BankUtil.BALANCE;

public class FailureIT extends ITBaseImpl {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  public static class NullObserver implements Observer {
    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {}
  }

  public static class FailuresObserverProvider implements ObserverProvider {
    @Override
    public void provide(Registry or, Context ctx) {
      or.forColumn(new Column("attr", "lastupdate"), STRONG).useObserver(new NullObserver());
    }
  }

  @Override
  protected Class<? extends ObserverProvider> getObserverProviderClass() {
    return FailuresObserverProvider.class;
  }

  @Test
  public void testRollbackMany() throws Exception {
    testRollbackMany(true);
  }

  @Test
  public void testRollbackManyTimeout() throws Exception {
    testRollbackMany(false);
  }

  private void testRollbackMany(boolean killTransactor) throws Exception {

    // test writing lots of columns that need to be rolled back

    Column col1 = new Column("fam1", "q1");
    Column col2 = new Column("fam1", "q2");

    TestTransaction tx = new TestTransaction(env);

    for (int r = 0; r < 10; r++) {
      String row = Integer.toString(r);
      tx.set(row, col1, "0" + r + "0");
      tx.set(row, col2, "0" + r + "1");
    }

    tx.done();

    TransactorNode t2 = new TransactorNode(env);
    TestTransaction tx2 = new TestTransaction(env, t2);

    for (int r = 0; r < 10; r++) {
      tx2.set(r + "", col1, "1" + r + "0");
      tx2.set(r + "", col2, "1" + r + "1");
    }

    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));

    if (killTransactor) {
      t2.close();
    }

    TestTransaction tx3 = new TestTransaction(env);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("0" + r + "0", tx3.gets(r + "", col1));
      Assert.assertEquals("0" + r + "1", tx3.gets(r + "", col2));
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
      Assert.assertEquals("0" + r + "0", tx4.gets(r + "", col1));
      Assert.assertEquals("0" + r + "1", tx4.gets(r + "", col2));
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

  private void testRollforwardMany(boolean killTransactor) throws Exception {
    // test writing lots of columns that need to be rolled forward

    Column col1 = new Column("fam1", "q1");
    Column col2 = new Column("fam1", "q2");

    TestTransaction tx = new TestTransaction(env);

    for (int r = 0; r < 10; r++) {
      tx.set(r + "", col1, "0" + r + "0");
      tx.set(r + "", col2, "0" + r + "1");
    }

    tx.done();

    TransactorNode t2 = new TransactorNode(env);
    TestTransaction tx2 = new TestTransaction(env, t2);

    for (int r = 0; r < 10; r++) {
      tx2.set(r + "", col1, "1" + r + "0");
      tx2.set(r + "", col2, "1" + r + "1");
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
      Assert.assertEquals("1" + r + "0", tx3.gets(r + "", col1));
      Assert.assertEquals("1" + r + "1", tx3.gets(r + "", col2));
    }

    tx2.finishCommit(cd, commitTs);

    TestTransaction tx4 = new TestTransaction(env);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("1" + r + "0", tx4.gets(r + "", col1));
      Assert.assertEquals("1" + r + "1", tx4.gets(r + "", col2));
    }

    if (!killTransactor) {
      t2.close();
    }
  }

  @Test
  public void testRollback() throws Exception {
    // test the case where a scan encounters a stuck lock and rolls it back

    TestTransaction tx = new TestTransaction(env);

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);

    int bal1 = Integer.parseInt(tx2.gets("bob", BALANCE));
    int bal2 = Integer.parseInt(tx2.gets("joe", BALANCE));

    tx2.set("bob", BALANCE, (bal1 - 7) + "");
    tx2.set("joe", BALANCE, (bal2 + 7) + "");

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

    Assert.assertEquals(bobBal + "", tx4.gets("bob", BALANCE));
    Assert.assertEquals(joeBal + "", tx4.gets("joe", BALANCE));
    Assert.assertEquals("67", tx4.gets("jill", BALANCE));

    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertFalse(tx2.commitPrimaryColumn(cd, commitTs));

    BankUtil.transfer(env, "bob", "joe", 2);
    bobBal -= 2;
    joeBal += 2;

    TestTransaction tx6 = new TestTransaction(env);

    Assert.assertEquals(bobBal + "", tx6.gets("bob", BALANCE));
    Assert.assertEquals(joeBal + "", tx6.gets("joe", BALANCE));
    Assert.assertEquals("67", tx6.gets("jill", BALANCE));
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

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");

    tx.done();

    TestTransaction tx2 = new TestTransaction(env, t1);

    int bal1 = Integer.parseInt(tx2.gets("bob", BALANCE));
    int bal2 = Integer.parseInt(tx2.gets("joe", BALANCE));

    tx2.set("bob", BALANCE, (bal1 - 7) + "");
    tx2.set("joe", BALANCE, (bal2 + 7) + "");

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

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);

    int bal1 = Integer.parseInt(tx2.gets("bob", BALANCE));
    int bal2 = Integer.parseInt(tx2.gets("joe", BALANCE));

    tx2.set("bob", BALANCE, (bal1 - 7) + "");
    tx2.set("joe", BALANCE, (bal2 + 7) + "");

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

    Assert.assertEquals(bobBal + "", tx4.gets("bob", BALANCE));
    Assert.assertEquals(joeBal + "", tx4.gets("joe", BALANCE));
    Assert.assertEquals("62", tx4.gets("jill", BALANCE));

    tx2.finishCommit(cd, commitTs);

    TestTransaction tx5 = new TestTransaction(env);

    Assert.assertEquals(bobBal + "", tx5.gets("bob", BALANCE));
    Assert.assertEquals(joeBal + "", tx5.gets("joe", BALANCE));
    Assert.assertEquals("62", tx5.gets("jill", BALANCE));
  }

  @Test
  public void testAcks() throws Exception {
    // TODO test that acks are properly handled in rollback and rollforward

    TestTransaction tx = new TestTransaction(env);

    final Column lastUpdate = new Column("attr", "lastupdate");
    final Column docContent = new Column("doc", "content");
    final Column docUrl = new Column("doc", "url");

    tx.set("url0000", lastUpdate, "3");
    tx.set("url0000", docContent, "abc def");

    tx.done();

    TestTransaction tx2 = new TestTransaction(env, "url0000", lastUpdate);
    tx2.set("idx:abc", docUrl, "url0000");
    tx2.set("idx:def", docUrl, "url0000");
    CommitData cd = tx2.createCommitData();
    tx2.preCommit(cd);

    TestTransaction tx3 = new TestTransaction(env);
    Assert.assertNull(tx3.gets("idx:abc", docUrl));
    Assert.assertNull(tx3.gets("idx:def", docUrl));
    Assert.assertEquals("3", tx3.gets("url0000", lastUpdate));

    Scanner scanner = env.getConnector().createScanner(env.getTable(), Authorizations.EMPTY);
    Notification.configureScanner(scanner);
    Iterator<Entry<Key, Value>> iter = scanner.iterator();
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals("url0000", iter.next().getKey().getRow().toString());

    TestTransaction tx5 = new TestTransaction(env, "url0000", lastUpdate);
    tx5.set("idx:abc", docUrl, "url0000");
    tx5.set("idx:def", docUrl, "url0000");
    cd = tx5.createCommitData();
    Assert.assertTrue(tx5.preCommit(cd));
    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(tx5.commitPrimaryColumn(cd, commitTs));

    // should roll tx5 forward
    TestTransaction tx6 = new TestTransaction(env);
    Assert.assertEquals("3", tx6.gets("url0000", lastUpdate));
    Assert.assertEquals("url0000", tx6.gets("idx:abc", docUrl));
    Assert.assertEquals("url0000", tx6.gets("idx:def", docUrl));

    iter = scanner.iterator();
    Assert.assertTrue(iter.hasNext());

    // TODO is tx4 start before tx5, then this test will not work because AlreadyAck is not thrown
    // for overlapping.. CommitException is thrown
    TestTransaction tx4 = new TestTransaction(env, "url0000", lastUpdate);
    tx4.set("idx:abc", docUrl, "url0000");
    tx4.set("idx:def", docUrl, "url0000");

    try {
      // should not go through if tx5 is properly rolled forward
      tx4.commit();
      Assert.fail();
    } catch (AlreadyAcknowledgedException aae) {
      // do nothing
    }

    // commit above should schedule async delete of notification
    env.getSharedResources().getBatchWriter().waitForAsyncFlush();
    iter = scanner.iterator();
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testStaleScanPrevention() throws Exception {

    TestTransaction tx = new TestTransaction(env);

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);
    Assert.assertEquals("10", tx2.gets("bob", BALANCE));

    BankUtil.transfer(env, "joe", "jill", 1);
    BankUtil.transfer(env, "joe", "bob", 1);
    BankUtil.transfer(env, "bob", "joe", 2);
    BankUtil.transfer(env, "jill", "joe", 2);

    conn.tableOperations().flush(table, null, null, true);

    Assert.assertEquals("20", tx2.gets("joe", BALANCE));

    // Stale scan should not occur due to oldest active timestamp tracking in Zookeeper
    tx2.close();

    TestTransaction tx3 = new TestTransaction(env);

    Assert.assertEquals("9", tx3.gets("bob", BALANCE));
    Assert.assertEquals("22", tx3.gets("joe", BALANCE));
    Assert.assertEquals("59", tx3.gets("jill", BALANCE));
  }

  @Test(timeout = 60000)
  public void testForcedStaleScan() throws Exception {

    TestTransaction tx = new TestTransaction(env);

    tx.set("bob", BALANCE, "10");
    tx.set("joe", BALANCE, "20");
    tx.set("jill", BALANCE, "60");
    tx.set("john", BALANCE, "3");

    tx.done();

    TestTransaction tx2 = new TestTransaction(env);
    Assert.assertEquals("10", tx2.gets("bob", BALANCE));

    TestTransaction tx3 = new TestTransaction(env);
    tx3.gets("john", BALANCE);

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
    Assert.assertNull(tx2.gets("joe", BALANCE));

    try {
      // closing should detect the stale scan
      tx2.close();
      Assert.assertFalse(true);
    } catch (StaleScanException sse) {
      // do nothing
    }

    tx3.set("john", BALANCE, "5");

    try {
      tx3.commit();
      Assert.assertFalse(true);
    } catch (CommitException e) {
      // should not throw an exception
      tx3.close();
    }

    TestTransaction tx4 = new TestTransaction(env);

    Assert.assertEquals("9", tx4.gets("bob", BALANCE));
    Assert.assertEquals("22", tx4.gets("joe", BALANCE));
    Assert.assertEquals("59", tx4.gets("jill", BALANCE));
    Assert.assertEquals("3", tx4.gets("john", BALANCE));
  }

  @Test
  public void testCommitBug1() throws Exception {

    TestTransaction tx1 = new TestTransaction(env);

    tx1.set("bob", BALANCE, "10");
    tx1.set("joe", BALANCE, "20");
    tx1.set("jill", BALANCE, "60");

    CommitData cd = tx1.createCommitData();
    Assert.assertTrue(tx1.preCommit(cd));

    while (true) {
      TestTransaction tx2 = new TestTransaction(env);

      tx2.set("bob", BALANCE, "11");
      tx2.set("jill", BALANCE, "61");

      // tx1 should be rolled back even in case where columns tx1 locked are not read by tx2
      try {
        tx2.commit();
        break;
      } catch (CommitException ce) {
        // do nothing
      }
    }

    TestTransaction tx4 = new TestTransaction(env);

    Assert.assertEquals("11", tx4.gets("bob", BALANCE));
    Assert.assertNull(tx4.gets("joe", BALANCE));
    Assert.assertEquals("61", tx4.gets("jill", BALANCE));
  }

  @Test
  public void testRollbackSelf() throws Exception {
    // test for #660... ensure when transaction rolls itself back that it properly sets the primary
    // flag

    TestTransaction tx1 = new TestTransaction(env);

    tx1.set("bob", BALANCE, "10");
    tx1.set("joe", BALANCE, "20");
    tx1.set("jill", BALANCE, "60");

    tx1.done();


    TestTransaction tx2 = new TestTransaction(env, "jill", BALANCE, 1);

    TestTransaction tx3 = new TestTransaction(env);
    TestUtil.increment(tx3, "bob", BALANCE, 5);
    TestUtil.increment(tx3, "joe", BALANCE, -5);
    tx3.done();

    TestUtil.increment(tx2, "bob", BALANCE, 5);
    TestUtil.increment(tx2, "jill", BALANCE, -5);

    // should be able to successfully lock the primary column jill... but then should fail to lock
    // bob and have to rollback
    try {
      tx2.commit();
      Assert.fail("Expected commit exception");
    } catch (CommitException ce) {
      // do nothing
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
