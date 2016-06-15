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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.config.ObserverConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;
import org.apache.fluo.api.types.TypedObserver;
import org.apache.fluo.api.types.TypedSnapshot;
import org.apache.fluo.api.types.TypedTransactionBase;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

public class WeakNotificationOverlapIT extends ITBaseImpl {

  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  public static class TotalObserver extends TypedObserver {

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(typeLayer.bc().fam("stat").qual("changed").vis(),
          NotificationType.WEAK);
    }

    @Override
    public void process(TypedTransactionBase tx, Bytes row, Column col) {
      Integer total = tx.get().row(row).fam("stat").qual("total").toInteger();
      if (total == null) {
        return;
      }
      int processed = tx.get().row(row).fam("stat").qual("processed").toInteger(0);

      tx.mutate().row(row).fam("stat").qual("processed").set(total);
      tx.mutate().row("all").fam("stat").qual("total").increment(total - processed);
    }
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Arrays.asList(new ObserverConfiguration(TotalObserver.class.getName()));
  }

  @Test
  public void testOverlap() throws Exception {
    // this test ensures that processing of weak notification deletes based on startTs and not
    // commitTs

    Column ntfyCol = typeLayer.bc().fam("stat").qual("changed").vis();

    TestTransaction ttx1 = new TestTransaction(env);
    ttx1.mutate().row(1).fam("stat").qual("total").increment(1);
    ttx1.mutate().row(1).col(ntfyCol).weaklyNotify();
    ttx1.done();

    TestTransaction ttx2 = new TestTransaction(env, "1", ntfyCol);

    TestTransaction ttx3 = new TestTransaction(env);
    ttx3.mutate().row(1).fam("stat").qual("total").increment(1);
    ttx3.mutate().row(1).col(ntfyCol).weaklyNotify();
    ttx3.done();

    Assert.assertEquals(1, countNotifications());

    new TotalObserver().process(ttx2, Bytes.of("1"), ntfyCol);
    // should not delete notification created by ttx3
    ttx2.done();

    TestTransaction snap1 = new TestTransaction(env);
    Assert.assertEquals(1, snap1.get().row("all").fam("stat").qual("total").toInteger(-1));
    snap1.done();

    Assert.assertEquals(1, countNotifications());

    TestTransaction ttx4 = new TestTransaction(env, "1", ntfyCol);
    new TotalObserver().process(ttx4, Bytes.of("1"), ntfyCol);
    ttx4.done();

    Assert.assertEquals(0, countNotifications());

    TestTransaction snap2 = new TestTransaction(env);
    Assert.assertEquals(2, snap2.get().row("all").fam("stat").qual("total").toInteger(-1));
    snap2.done();

    // the following code is a repeat of the above with a slight diff. The following tx creates a
    // notification, but deletes the data so there is no work for the
    // observer. This test the case where a observer deletes a notification w/o making any updates.
    TestTransaction ttx5 = new TestTransaction(env);
    ttx5.mutate().row(1).fam("stat").qual("total").delete();
    ttx5.mutate().row(1).fam("stat").qual("processed").delete();
    ttx5.mutate().row(1).col(ntfyCol).weaklyNotify();
    ttx5.done();

    Assert.assertEquals(1, countNotifications());

    TestTransaction ttx6 = new TestTransaction(env, "1", ntfyCol);

    TestTransaction ttx7 = new TestTransaction(env);
    ttx7.mutate().row(1).fam("stat").qual("total").increment(1);
    ttx7.mutate().row(1).col(ntfyCol).weaklyNotify();
    ttx7.done();

    Assert.assertEquals(1, countNotifications());

    new TotalObserver().process(ttx6, Bytes.of("1"), ntfyCol);
    // should not delete notification created by ttx7
    ttx6.done();

    Assert.assertEquals(1, countNotifications());

    TestTransaction snap3 = new TestTransaction(env);
    Assert.assertEquals(2, snap3.get().row("all").fam("stat").qual("total").toInteger(-1));
    snap3.done();

    TestTransaction ttx8 = new TestTransaction(env, "1", ntfyCol);
    new TotalObserver().process(ttx8, Bytes.of("1"), ntfyCol);
    ttx8.done();

    Assert.assertEquals(0, countNotifications());

    TestTransaction snap4 = new TestTransaction(env);
    Assert.assertEquals(3, snap4.get().row("all").fam("stat").qual("total").toInteger(-1));
    snap4.done();
  }

  @Test
  public void testOverlap2() throws Exception {
    // this test ensures that setting weak notification is based on commitTs and not startTs

    Column ntfyCol = typeLayer.bc().fam("stat").qual("changed").vis();

    TestTransaction ttx1 = new TestTransaction(env);
    ttx1.mutate().row(1).fam("stat").qual("total").increment(1);
    ttx1.mutate().row(1).col(ntfyCol).weaklyNotify();
    ttx1.done();

    Assert.assertEquals(1, countNotifications());

    TestTransaction ttx2 = new TestTransaction(env);
    ttx2.mutate().row(1).fam("stat").qual("total").increment(1);
    ttx2.mutate().row(1).col(ntfyCol).weaklyNotify();
    CommitData cd2 = ttx2.createCommitData();
    Assert.assertTrue(ttx2.preCommit(cd2));

    // simulate an observer processing the notification created by ttx1 while ttx2 is in the middle
    // of committing. Processing this observer should not delete
    // the notification for ttx2. It should delete the notification for ttx1.
    TestTransaction ttx3 = new TestTransaction(env, "1", ntfyCol);

    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(ttx2.commitPrimaryColumn(cd2, commitTs));
    ttx2.finishCommit(cd2, commitTs);
    ttx2.close();

    Assert.assertEquals(1, countNotifications());

    new TotalObserver().process(ttx3, Bytes.of("1"), ntfyCol);
    ttx3.done();

    Assert.assertEquals(1, countNotifications());
    try (TypedSnapshot snapshot = typeLayer.wrap(client.newSnapshot())) {
      Assert.assertEquals(1, snapshot.get().row("all").fam("stat").qual("total").toInteger(-1));
    }

    TestTransaction ttx4 = new TestTransaction(env, "1", ntfyCol);
    new TotalObserver().process(ttx4, Bytes.of("1"), ntfyCol);
    ttx4.done();

    Assert.assertEquals(0, countNotifications());
    try (TypedSnapshot snapshot = typeLayer.wrap(client.newSnapshot())) {
      Assert.assertEquals(2, snapshot.get().row("all").fam("stat").qual("total").toInteger(-1));
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
