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

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.api.observer.StringObserver;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.apache.fluo.integration.TestUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.fluo.api.observer.Observer.NotificationType.WEAK;

public class WeakNotificationOverlapIT extends ITBaseImpl {

  private static final Column STAT_TOTAL = new Column("stat", "total");
  private static final Column STAT_PROCESSED = new Column("stat", "processed");
  private static final Column STAT_CHANGED = new Column("stat", "changed");

  private static final StringObserver TOTAL_OBSERVER = (tx, row, col) -> {
    String totalStr = tx.gets(row, STAT_TOTAL);
    if (totalStr == null) {
      return;
    }
    Integer total = Integer.parseInt(totalStr);
    int processed = TestUtil.getOrDefault(tx, row, STAT_PROCESSED, 0);
    tx.set(row, new Column("stat", "processed"), total + "");
    TestUtil.increment(tx, "all", new Column("stat", "total"), total - processed);
  };

  public static class WeakNtfyObserverProvider implements ObserverProvider {
    @Override
    public void provide(Registry or, Context ctx) {
      or.forColumn(STAT_CHANGED, WEAK).useStrObserver(TOTAL_OBSERVER);
    }
  }

  @Override
  protected Class<? extends ObserverProvider> getObserverProviderClass() {
    return WeakNtfyObserverProvider.class;
  }

  @Test
  public void testOverlap() throws Exception {
    // this test ensures that processing of weak notification deletes based on startTs and not
    // commitTs

    TestTransaction ttx1 = new TestTransaction(env);
    TestUtil.increment(ttx1, "1", STAT_TOTAL, 1);
    ttx1.setWeakNotification("1", STAT_CHANGED);
    ttx1.done();

    TestTransaction ttx2 = new TestTransaction(env, "1", STAT_CHANGED);

    TestTransaction ttx3 = new TestTransaction(env);
    TestUtil.increment(ttx3, "1", STAT_TOTAL, 1);
    ttx3.setWeakNotification("1", STAT_CHANGED);
    ttx3.done();

    Assert.assertEquals(1, countNotifications());

    TOTAL_OBSERVER.process(ttx2, Bytes.of("1"), STAT_CHANGED);
    // should not delete notification created by ttx3
    ttx2.done();

    TestTransaction snap1 = new TestTransaction(env);
    Assert.assertEquals("1", snap1.gets("all", STAT_TOTAL));
    snap1.done();

    Assert.assertEquals(1, countNotifications());

    TestTransaction ttx4 = new TestTransaction(env, "1", STAT_CHANGED);
    TOTAL_OBSERVER.process(ttx4, Bytes.of("1"), STAT_CHANGED);
    ttx4.done();

    Assert.assertEquals(0, countNotifications());

    TestTransaction snap2 = new TestTransaction(env);
    Assert.assertEquals("2", snap2.gets("all", STAT_TOTAL));
    snap2.done();

    // the following code is a repeat of the above with a slight diff. The following tx creates a
    // notification, but deletes the data so there is no work for the
    // observer. This test the case where a observer deletes a notification w/o making any updates.
    TestTransaction ttx5 = new TestTransaction(env);
    ttx5.delete("1", STAT_TOTAL);
    ttx5.delete("1", STAT_PROCESSED);
    ttx5.setWeakNotification("1", STAT_CHANGED);
    ttx5.done();

    Assert.assertEquals(1, countNotifications());

    TestTransaction ttx6 = new TestTransaction(env, "1", STAT_CHANGED);

    TestTransaction ttx7 = new TestTransaction(env);
    TestUtil.increment(ttx7, "1", STAT_TOTAL, 1);
    ttx7.setWeakNotification("1", STAT_CHANGED);
    ttx7.done();

    Assert.assertEquals(1, countNotifications());

    TOTAL_OBSERVER.process(ttx6, Bytes.of("1"), STAT_CHANGED);
    // should not delete notification created by ttx7
    ttx6.done();

    Assert.assertEquals(1, countNotifications());

    TestTransaction snap3 = new TestTransaction(env);
    Assert.assertEquals("2", snap3.gets("all", STAT_TOTAL));
    snap3.done();

    TestTransaction ttx8 = new TestTransaction(env, "1", STAT_CHANGED);
    TOTAL_OBSERVER.process(ttx8, Bytes.of("1"), STAT_CHANGED);
    ttx8.done();

    Assert.assertEquals(0, countNotifications());

    TestTransaction snap4 = new TestTransaction(env);
    Assert.assertEquals("3", snap4.gets("all", STAT_TOTAL));
    snap4.done();
  }

  @Test
  public void testOverlap2() throws Exception {
    // this test ensures that setting weak notification is based on commitTs and not startTs

    TestTransaction ttx1 = new TestTransaction(env);
    TestUtil.increment(ttx1, "1", STAT_TOTAL, 1);
    ttx1.setWeakNotification("1", STAT_CHANGED);
    ttx1.done();

    Assert.assertEquals(1, countNotifications());

    TestTransaction ttx2 = new TestTransaction(env);
    TestUtil.increment(ttx2, "1", STAT_TOTAL, 1);
    ttx2.setWeakNotification("1", STAT_CHANGED);
    CommitData cd2 = ttx2.createCommitData();
    Assert.assertTrue(ttx2.preCommit(cd2));

    // simulate an observer processing the notification created by ttx1 while ttx2 is in the middle
    // of committing. Processing this observer should not delete
    // the notification for ttx2. It should delete the notification for ttx1.
    TestTransaction ttx3 = new TestTransaction(env, "1", STAT_CHANGED);

    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(ttx2.commitPrimaryColumn(cd2, commitTs));
    ttx2.finishCommit(cd2, commitTs);
    ttx2.close();

    Assert.assertEquals(1, countNotifications());

    TOTAL_OBSERVER.process(ttx3, Bytes.of("1"), STAT_CHANGED);
    ttx3.done();

    Assert.assertEquals(1, countNotifications());
    try (Snapshot snapshot = client.newSnapshot()) {
      Assert.assertEquals("1", snapshot.gets("all", STAT_TOTAL));
    }

    TestTransaction ttx4 = new TestTransaction(env, "1", STAT_CHANGED);
    TOTAL_OBSERVER.process(ttx4, Bytes.of("1"), STAT_CHANGED);
    ttx4.done();

    Assert.assertEquals(0, countNotifications());
    try (Snapshot snapshot = client.newSnapshot()) {
      Assert.assertEquals("2", snapshot.gets("all", STAT_TOTAL));
    }
  }

  private int countNotifications() throws Exception {
    // deletes of notifications are queued async at end of transaction
    env.getSharedResources().getBatchWriter().waitForAsyncFlush();

    Scanner scanner = conn.createScanner(getCurTableName(), Authorizations.EMPTY);
    Notification.configureScanner(scanner);

    int count = 0;
    for (Iterator<Entry<Key, Value>> iterator = scanner.iterator(); iterator.hasNext();) {
      iterator.next();
      count++;
    }
    return count;
  }
}
