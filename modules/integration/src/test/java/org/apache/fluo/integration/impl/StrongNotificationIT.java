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

import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.impl.TransactorNode;
import org.apache.fluo.integration.ITBaseMini;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.fluo.api.observer.Observer.NotificationType.STRONG;

public class StrongNotificationIT extends ITBaseMini {

  private static final Column OC = new Column("f", "q");
  private static final Column RC = new Column("f", "r");

  public static class StrongNtfyObserverProvider implements ObserverProvider {
    @Override
    public void provide(Registry or, Context ctx) {
      or.forColumn(OC, STRONG).useObserver((tx, row, col) -> {
        Bytes v = tx.get(row, col);
        tx.set(v, RC, row);
      });
    }
  }

  @Override
  protected Class<? extends ObserverProvider> getObserverProviderClass() {
    return StrongNtfyObserverProvider.class;
  }

  @Test
  public void testRollforward() throws Exception {
    // test for bug #642
    try (Environment env = new Environment(config);
        TransactorNode tnode = new TransactorNode(env)) {
      TestTransaction tx = new TestTransaction(env, tnode);

      // set three columns that should each trigger observers
      tx.set("abc", OC, "xyz");
      tx.set("def", OC, "uvw");
      tx.set("123", OC, "890");

      // partially commit transaction
      CommitData cd = tx.createCommitData();
      Assert.assertTrue(tx.preCommit(cd));
      Assert.assertTrue(
          tx.commitPrimaryColumn(cd, env.getSharedResources().getOracleClient().getStamp()));
      tx.close();
    }

    miniFluo.waitForObservers();

    try (Snapshot snap = client.newSnapshot()) {
      Assert.assertEquals("abc", snap.gets("xyz", RC));
      Assert.assertEquals("def", snap.gets("uvw", RC));
      Assert.assertEquals("123", snap.gets("890", RC));

      Assert.assertEquals("xyz", snap.gets("abc", OC));
      Assert.assertEquals("uvw", snap.gets("def", OC));
      Assert.assertEquals("890", snap.gets("123", OC));
    }
  }
}
