/*
 * Copyright 2016 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.integration.impl;

import java.util.Collections;
import java.util.List;

import io.fluo.api.client.Snapshot;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.TransactionImpl.CommitData;
import io.fluo.core.impl.TransactorNode;
import io.fluo.integration.ITBaseMini;
import io.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

public class StrongNotificationIT extends ITBaseMini {

  private static final Column OC = new Column("f", "q");
  private static final Column RC = new Column("f", "r");

  public static class SimpleObserver extends AbstractObserver {

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
      String r = row.toString();

      String v = tx.gets(r, col);
      tx.set(v, RC, r);
    }

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(OC, NotificationType.STRONG);
    }
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(SimpleObserver.class.getName()));
  }

  @Test
  public void testRollforward() throws Exception {
    // test for bug #642
    try (Environment env = new Environment(config); TransactorNode tnode = new TransactorNode(env)) {
      TestTransaction tx = new TestTransaction(env, tnode);

      // set three columns that should each trigger observers
      tx.set("abc", OC, "xyz");
      tx.set("def", OC, "uvw");
      tx.set("123", OC, "890");

      // partially commit transaction
      CommitData cd = tx.createCommitData();
      Assert.assertTrue(tx.preCommit(cd));
      Assert.assertTrue(tx.commitPrimaryColumn(cd, env.getSharedResources().getOracleClient()
          .getStamp()));
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
