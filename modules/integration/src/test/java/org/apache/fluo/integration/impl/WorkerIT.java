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

import com.google.common.collect.Iterables;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.api.observer.StringObserver;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.observer.Observers;
import org.apache.fluo.core.worker.NotificationFinder;
import org.apache.fluo.core.worker.finder.hash.PartitionNotificationFinder;
import org.apache.fluo.integration.ITBaseMini;
import org.apache.fluo.integration.TestTransaction;
import org.apache.fluo.mini.MiniFluoImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.apache.fluo.api.observer.Observer.NotificationType.STRONG;

/**
 * A simple test that added links between nodes in a graph. There is an observer that updates an
 * index of node degree.
 */
public class WorkerIT extends ITBaseMini {
  @Rule
  public Timeout globalTimeout = Timeout.seconds(getTestTimeout() * 2);
  // timeout needs to be > 60secs for testMultipleFinders()
  private static final Column LAST_UPDATE = new Column("attr", "lastupdate");
  private static final Column DEGREE = new Column("attr", "degree");

  private static Column observedColumn = LAST_UPDATE;

  public static class DegreeIndexer implements StringObserver {

    @Override
    public void process(TransactionBase tx, String row, Column col) throws Exception {
      // get previously calculated degree
      String degree = tx.gets(row, DEGREE);

      // calculate new degree
      String degree2 = "" + Iterables.size(tx.scanner().over(row, new Column("link")).build());

      if (degree == null || !degree.equals(degree2)) {
        tx.set(row, DEGREE, degree2);

        // put new entry in degree index
        tx.set("IDEG" + degree2, new Column("node", row), "");
      }

      if (degree != null) {
        // delete old degree in index
        tx.delete("IDEG" + degree, new Column("node", row));
      }
    }
  }

  public static class WorkerITObserverProvider implements ObserverProvider {
    @Override
    public void provide(Registry or, Context ctx) {
      or.forColumn(observedColumn, STRONG).useObserver(new DegreeIndexer());
    }
  }

  @Override
  protected Class<? extends ObserverProvider> getObserverProviderClass() {
    return WorkerITObserverProvider.class;
  }

  @Test
  public void test1() throws Exception {

    Environment env = new Environment(config);

    addLink("N0003", "N0040");
    addLink("N0003", "N0020");

    miniFluo.waitForObservers();

    // verify observer updated degree index
    TestTransaction tx3 = new TestTransaction(env);
    Assert.assertEquals("2", tx3.gets("N0003", DEGREE));
    Assert.assertEquals("", tx3.gets("IDEG2", new Column("node", "N0003")));

    // add a link between two nodes in a graph
    tx3.set("N0003", new Column("link", "N0010"), "");
    tx3.set("N0003", LAST_UPDATE, System.currentTimeMillis() + "");
    tx3.done();

    miniFluo.waitForObservers();

    // verify observer updated degree index. Should have deleted old index entry
    // and added a new one
    TestTransaction tx4 = new TestTransaction(env);
    Assert.assertEquals("3", tx4.gets("N0003", DEGREE));
    Assert.assertNull("", tx4.gets("IDEG2", new Column("node", "N0003")));
    Assert.assertEquals("", tx4.gets("IDEG3", new Column("node", "N0003")));

    // test rollback
    TestTransaction tx5 = new TestTransaction(env);
    tx5.set("N0003", new Column("link", "N0030"), "");
    tx5.set("N0003", LAST_UPDATE, System.currentTimeMillis() + "");
    tx5.done();

    TestTransaction tx6 = new TestTransaction(env);
    tx6.set("N0003", new Column("link", "N0050"), "");
    tx6.set("N0003", LAST_UPDATE, System.currentTimeMillis() + "");
    CommitData cd = tx6.createCommitData();
    tx6.preCommit(cd, new RowColumn("N0003", LAST_UPDATE));

    miniFluo.waitForObservers();

    TestTransaction tx7 = new TestTransaction(env);
    Assert.assertEquals("4", tx7.gets("N0003", DEGREE));
    Assert.assertNull("", tx7.gets("IDEG3", new Column("node", "N0003")));
    Assert.assertEquals("", tx7.gets("IDEG4", new Column("node", "N0003")));

    env.close();
  }

  /*
   * test when the configured column of an observer stored in zk differs from what the class returns
   */
  @Test
  public void testDiffObserverConfig() throws Exception {
    observedColumn = new Column("attr2", "lastupdate");
    try {
      try (Environment env = new Environment(config);
          Observers op = env.getConfiguredObservers().getObservers(env)) {
        op.getObserver(LAST_UPDATE);
      }

      Assert.fail();

    } catch (IllegalArgumentException ise) {
      Assert.assertTrue(ise.getMessage()
          .contains("Column attr2 lastupdate  not previously configured for strong notifications"));
    } finally {
      observedColumn = LAST_UPDATE;
    }
  }

  private void addLink(String from, String to) {
    try (Transaction tx = client.newTransaction()) {
      tx.set(from, new Column("link", to), "");
      tx.set(from, LAST_UPDATE, System.currentTimeMillis() + "");
      tx.commit();
    }
  }

  @Test
  public void testMultipleFinders() {

    try (Environment env = new Environment(config)) {

      NotificationFinder nf1 = new PartitionNotificationFinder();
      nf1.init(env, ((MiniFluoImpl) miniFluo).getNotificationProcessor());
      nf1.start();

      NotificationFinder nf2 = new PartitionNotificationFinder();
      nf2.init(env, ((MiniFluoImpl) miniFluo).getNotificationProcessor());
      nf2.start();

      for (int i = 0; i < 10; i++) {
        addLink("N0003", "N00" + i + "0");
      }

      miniFluo.waitForObservers();

      try (Snapshot snap = client.newSnapshot()) {
        Assert.assertEquals("10", snap.gets("N0003", DEGREE));
        Assert.assertEquals("", snap.gets("IDEG10", new Column("node", "N0003")));
      }

      nf2.stop();

      for (int i = 1; i < 10; i++) {
        addLink("N0003", "N0" + i + "00");
      }

      miniFluo.waitForObservers();

      try (Snapshot snap = client.newSnapshot()) {
        Assert.assertEquals("19", snap.gets("N0003", DEGREE));
        Assert.assertEquals("", snap.gets("IDEG19", new Column("node", "N0003")));
        Assert.assertNull(snap.gets("IDEG10", new Column("node", "N0003")));
      }

      nf1.stop();
    }
  }

  // TODO test that observers trigger on delete
}
