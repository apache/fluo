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

import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.integration.ITBaseMini;
import org.apache.fluo.integration.TestTransaction;
import org.apache.fluo.integration.TestUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.fluo.api.observer.Observer.NotificationType.WEAK;

public class WeakNotificationIT extends ITBaseMini {

  private static final Column STAT_COUNT = new Column("stat", "count");
  private static final Column STAT_CHECK = new Column("stat", "check");

  public static class SimpleObserver implements Observer {
    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {

      CellScanner cellScanner =
          tx.scanner().over(Span.exact(row, new Column(Bytes.of("stats")))).build();

      int sum = 0;

      for (RowColumnValue rcv : cellScanner) {
        sum += Integer.parseInt(rcv.getValue().toString());
        tx.delete(row, rcv.getColumn());
      }

      if (sum != 0) {
        sum += TestUtil.getOrDefault(tx, row.toString(), STAT_COUNT, 0);
        tx.set(row.toString(), STAT_COUNT, sum + "");
      }
    }
  }

  public static class WeakNotificationITObserverProvider implements ObserverProvider {
    @Override
    public void provide(Registry or, Context ctx) {
      or.forColumn(STAT_CHECK, WEAK).useObserver(new SimpleObserver());
    }
  }

  @Override
  protected Class<? extends ObserverProvider> getObserverProviderClass() {
    return WeakNotificationITObserverProvider.class;
  }

  @Test
  public void testWeakNotification() throws Exception {
    Environment env = new Environment(config);

    TestTransaction tx1 = new TestTransaction(env);
    tx1.set("r1", STAT_COUNT, "3");
    tx1.done();

    TestTransaction tx2 = new TestTransaction(env);
    tx2.set("r1", new Column("stats", "af89"), "5");
    tx2.setWeakNotification("r1", STAT_CHECK);
    tx2.done();

    TestTransaction tx3 = new TestTransaction(env);
    tx3.set("r1", new Column("stats", "af99"), "7");
    tx3.setWeakNotification("r1", STAT_CHECK);
    tx3.done();

    miniFluo.waitForObservers();

    TestTransaction tx4 = new TestTransaction(env);
    Assert.assertEquals("15", tx4.gets("r1", STAT_COUNT));

    // overlapping transactions that set a weak notification should commit w/ no problem
    TestTransaction tx5 = new TestTransaction(env);
    tx5.set("r1", new Column("stats", "bff7"), "11");
    tx5.setWeakNotification("r1", STAT_CHECK);
    CommitData cd5 = tx5.createCommitData();
    Assert.assertTrue(tx5.preCommit(cd5));

    TestTransaction tx6 = new TestTransaction(env);
    tx6.set("r1", new Column("stats", "bff0"), "13");
    tx6.setWeakNotification("r1", STAT_CHECK);
    CommitData cd6 = tx6.createCommitData();
    Assert.assertTrue(tx6.preCommit(cd6));

    Stamp commitTs5 = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(tx5.commitPrimaryColumn(cd5, commitTs5));

    Stamp commitTs6 = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(tx6.commitPrimaryColumn(cd6, commitTs6));

    tx6.finishCommit(cd6, commitTs6);
    tx5.finishCommit(cd5, commitTs5);

    miniFluo.waitForObservers();

    TestTransaction tx7 = new TestTransaction(env);
    Assert.assertEquals("39", tx7.gets("r1", STAT_COUNT));

    env.close();
  }

  @Test(timeout = 30000)
  public void testNOOP() throws Exception {
    // if an observer makes not updates in a transaction, it should still delete the weak
    // notification
    try (Transaction tx1 = client.newTransaction()) {
      tx1.set("r1", STAT_COUNT, "3");
      tx1.setWeakNotification("r1", STAT_CHECK);
      tx1.commit();
    }

    // the following will loop forever if weak notification is not deleted
    miniFluo.waitForObservers();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadColumn() throws Exception {
    try (Transaction tx1 = client.newTransaction()) {
      tx1.set("r1", STAT_COUNT, "3");
      tx1.setWeakNotification("r1", new Column("stat", "foo"));
      tx1.commit();
    }
  }

}
