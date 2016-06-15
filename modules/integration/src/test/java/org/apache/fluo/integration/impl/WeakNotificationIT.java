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

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.ObserverConfiguration;
import org.apache.fluo.api.config.ScannerConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.iterator.ColumnIterator;
import org.apache.fluo.api.iterator.RowIterator;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;
import org.apache.fluo.api.types.TypedTransaction;
import org.apache.fluo.api.types.TypedTransactionBase;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.integration.ITBaseMini;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

public class WeakNotificationIT extends ITBaseMini {

  private static TypeLayer tl = new TypeLayer(new StringEncoder());

  public static class SimpleObserver extends AbstractObserver {

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
      TypedTransactionBase ttx = tl.wrap(tx);

      ScannerConfiguration sc = new ScannerConfiguration();
      sc.setSpan(Span.exact(row, new Column(Bytes.of("stats"))));
      RowIterator rowIter = ttx.get(sc);

      int sum = 0;

      if (rowIter.hasNext()) {
        ColumnIterator colIter = rowIter.next().getValue();
        while (colIter.hasNext()) {
          Entry<Column, Bytes> colVal = colIter.next();
          sum += Integer.parseInt(colVal.getValue().toString());
          ttx.delete(row, colVal.getKey());
        }
      }

      if (sum != 0) {
        sum += ttx.get().row(row).fam("stat").qual("count").toInteger(0);
        ttx.mutate().row(row).fam("stat").qual("count").set(sum);
      }
    }

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(tl.bc().fam("stat").qual("check").vis(), NotificationType.WEAK);
    }
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(SimpleObserver.class.getName()));
  }

  @Test
  public void testWeakNotification() throws Exception {
    Environment env = new Environment(config);

    TestTransaction tx1 = new TestTransaction(env);
    tx1.mutate().row("r1").fam("stat").qual("count").set(3);
    tx1.done();

    TestTransaction tx2 = new TestTransaction(env);
    tx2.mutate().row("r1").fam("stats").qual("af89").set(5);
    tx2.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
    tx2.done();

    TestTransaction tx3 = new TestTransaction(env);
    tx3.mutate().row("r1").fam("stats").qual("af99").set(7);
    tx3.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
    tx3.done();

    miniFluo.waitForObservers();

    TestTransaction tx4 = new TestTransaction(env);
    Assert.assertEquals(15, tx4.get().row("r1").fam("stat").qual("count").toInteger(0));

    // overlapping transactions that set a weak notification should commit w/ no problem
    TestTransaction tx5 = new TestTransaction(env);
    tx5.mutate().row("r1").fam("stats").qual("bff7").set(11);
    tx5.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
    CommitData cd5 = tx5.createCommitData();
    Assert.assertTrue(tx5.preCommit(cd5));

    TestTransaction tx6 = new TestTransaction(env);
    tx6.mutate().row("r1").fam("stats").qual("bff0").set(13);
    tx6.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
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
    Assert.assertEquals(39, tx7.get().row("r1").fam("stat").qual("count").toInteger(0));

    env.close();
  }

  @Test(timeout = 30000)
  public void testNOOP() throws Exception {
    // if an observer makes not updates in a transaction, it should still delete the weak
    // notification
    try (TypedTransaction tx1 = tl.wrap(client.newTransaction())) {
      tx1.mutate().row("r1").fam("stat").qual("count").set(3);
      tx1.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
      tx1.commit();
    }

    // the following will loop forever if weak notification is not deleted
    miniFluo.waitForObservers();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadColumn() throws Exception {
    try (TypedTransaction tx1 = tl.wrap(client.newTransaction())) {
      tx1.mutate().row("r1").fam("stat").qual("count").set(3);
      tx1.mutate().row("r1").fam("stat").qual("foo").weaklyNotify();
      tx1.commit();
    }
  }

}
