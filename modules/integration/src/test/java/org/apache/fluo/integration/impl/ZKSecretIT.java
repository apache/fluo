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


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.integration.ITBaseMini;
import org.apache.zookeeper.KeeperException.NoAuthException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.fluo.api.observer.Observer.NotificationType.STRONG;

public class ZKSecretIT extends ITBaseMini {

  public static class MyObserverProvider implements ObserverProvider {

    @Override
    public void provide(Registry or, Context ctx) {
      or.forColumn(new Column("edge", "forward"), STRONG).useObserver((tx, row, col) -> {
        tx.set(tx.get(row, col), new Column("edge", "reverese"), row);
      });
    }

  }

  @Override
  protected void setConfig(FluoConfiguration config) {
    config.setZookeeperSecret("are3");
    config.setObserverProvider(MyObserverProvider.class);
  }

  private ZooKeeper getZookeeper() throws IOException {
    ZooKeeper zk = new ZooKeeper(config.getAppZookeepers(), 30000, null);

    long start = System.currentTimeMillis();
    while (!zk.getState().isConnected() && System.currentTimeMillis() - start < 30000) {
      Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
    }

    return zk;
  }

  @Test
  public void testClientWithoutZKSecret() {
    try (FluoClient client = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      try (Transaction tx = client.newTransaction()) {
        tx.set("node08", new Column("edge", "forward"), "node75");
        tx.commit();
      }

      miniFluo.waitForObservers();
    }

    FluoConfiguration conf = new FluoConfiguration(miniFluo.getClientConfiguration());
    conf.setZookeeperSecret("");
    try (FluoClient client = FluoFactory.newClient(conf)) {
      Assert.fail("Expected client creation to fail.");
    } catch (Exception e) {
      boolean sawNoAuth = false;
      Throwable throwable = e;
      while (throwable != null) {
        if (throwable instanceof NoAuthException) {
          sawNoAuth = true;
          break;
        }
        throwable = throwable.getCause();
      }

      Assert.assertTrue(sawNoAuth);
    }

  }

  @Test
  public void testZKAcls() throws Exception {

    // verify basic functionality works when password is set in ZK
    try (FluoClient client = FluoFactory.newClient(miniFluo.getClientConfiguration())) {
      try (Transaction tx = client.newTransaction()) {
        tx.set("node08", new Column("edge", "forward"), "node75");
        tx.commit();
      }

      miniFluo.waitForObservers();

      try (Snapshot snap = client.newSnapshot()) {
        Assert.assertEquals("node08", snap.gets("node75", new Column("edge", "reverese")));
      }
    }

    ZooKeeper zk = getZookeeper();


    // Verify oracle gc timestamp is visible w/o a password. The GC iterator that runs in Accumulo
    // tablet servers reads this.
    String ts =
        new String(zk.getData(ZookeeperPath.ORACLE_GC_TIMESTAMP, false, null),
            StandardCharsets.UTF_8);
    Assert.assertTrue(ts.matches("\\d+"));

    // the timestamp should be read only... trying to modify it should fail
    try {
      zk.delete(ZookeeperPath.ORACLE_GC_TIMESTAMP, -1);
      Assert.fail();
    } catch (NoAuthException nae) {
    }

    try {
      zk.setData(ZookeeperPath.ORACLE_GC_TIMESTAMP, "foo".getBytes(), -1);
      Assert.fail();
    } catch (NoAuthException nae) {
    }

    // try accessing a few random nodes in ZK... All should fail.
    for (String path : Arrays.asList(ZookeeperPath.ORACLE_SERVER, ZookeeperPath.CONFIG_SHARED,
        ZookeeperPath.CONFIG_FLUO_OBSERVERS2, ZookeeperPath.TRANSACTOR_NODES)) {

      try {
        zk.getData(path, false, null);
        Assert.fail();
      } catch (NoAuthException nae) {
      }


      try {
        zk.getChildren(path, false);
      } catch (NoAuthException nae) {
      }

      try {
        zk.delete(path, -1);
        Assert.fail();
      } catch (NoAuthException nae) {
      }

      try {
        zk.setData(path, "foo".getBytes(), -1);
        Assert.fail();
      } catch (NoAuthException nae) {
      }
    }

    zk.close();

  }
}
