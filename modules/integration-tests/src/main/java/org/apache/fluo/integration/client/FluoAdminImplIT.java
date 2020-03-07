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

package org.apache.fluo.integration.client;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.accumulo.util.ZookeeperUtil;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoAdmin.AlreadyInitializedException;
import org.apache.fluo.api.client.FluoAdmin.InitializationOptions;
import org.apache.fluo.api.client.FluoAdmin.TableExistsException;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.service.FluoWorker;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.oracle.OracleServer;
import org.apache.fluo.core.util.AccumuloUtil;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.fluo.core.worker.FluoWorkerImpl;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FluoAdminImplIT extends ITBaseImpl {
  @Rule
  public Timeout globalTimeout = Timeout.seconds(getTestTimeout());

  @Test
  public void testInitializeTwiceFails() throws Exception {

    // stop oracle to avoid spurious exceptions when initializing
    oserver.stop();

    try (FluoAdmin admin = new FluoAdminImpl(config)) {

      InitializationOptions opts =
          new InitializationOptions().setClearZookeeper(true).setClearTable(true);

      admin.initialize(opts);
      admin.initialize(opts);

      opts.setClearZookeeper(false).setClearTable(false);
      try {
        admin.initialize(opts);
        fail("This should have failed");
      } catch (AlreadyInitializedException e) {
        // expected
      }

      opts.setClearZookeeper(false).setClearTable(true);
      try {
        admin.initialize(opts);
        fail("This should have failed");
      } catch (AlreadyInitializedException e) {
        // expected
      }

      opts.setClearZookeeper(true).setClearTable(false);
      try {
        admin.initialize(opts);
        fail("This should have failed");
      } catch (TableExistsException e) {
        // expected
      }
    }

    assertTrue(aClient.tableOperations().exists(config.getAccumuloTable()));
  }

  @Test
  public void testInitializeConfig() throws Exception {

    // stop oracle to avoid spurious exceptions when initializing
    oserver.stop();

    FluoConfiguration localConfig = new FluoConfiguration(config);
    localConfig.setProperty("fluo.test123", "${fluo.connection.application.name}");
    Assert.assertEquals(localConfig.getApplicationName(), localConfig.getString("fluo.test123"));

    try (FluoAdmin admin = new FluoAdminImpl(localConfig)) {

      InitializationOptions opts =
          new InitializationOptions().setClearZookeeper(true).setClearTable(true);
      admin.initialize(opts);

      // verify locality groups were set on the table
      try (AccumuloClient client = AccumuloUtil.getClient(config)) {
        Map<String, Set<Text>> localityGroups =
            client.tableOperations().getLocalityGroups(config.getAccumuloTable());
        Assert.assertEquals("Unexpected locality group count.", 1, localityGroups.size());
        Entry<String, Set<Text>> localityGroup = localityGroups.entrySet().iterator().next();
        Assert.assertEquals("'notify' locality group not found.",
            ColumnConstants.NOTIFY_LOCALITY_GROUP_NAME, localityGroup.getKey());
        Assert.assertEquals("'notify' locality group does not contain exactly 1 column family.", 1,
            localityGroup.getValue().size());
        Text colFam = localityGroup.getValue().iterator().next();
        Assert.assertTrue("'notify' locality group does not contain the correct column family.",
            ColumnConstants.NOTIFY_CF.contentEquals(colFam.getBytes(), 0, colFam.getLength()));
      }
    }

    try (FluoClientImpl client = new FluoClientImpl(localConfig)) {
      FluoConfiguration sharedConfig = client.getSharedConfiguration();
      Assert.assertEquals(localConfig.getApplicationName(), sharedConfig.getString("fluo.test123"));
      Assert.assertEquals(localConfig.getApplicationName(), sharedConfig.getApplicationName());
    }
  }

  @Test
  public void testInitializeWithNoChroot() throws Exception {

    // stop oracle to avoid spurious exceptions when initializing
    oserver.stop();

    InitializationOptions opts =
        new InitializationOptions().setClearZookeeper(true).setClearTable(true);

    for (String host : new String[] {"localhost", "localhost/", "localhost:9999",
        "localhost:9999/"}) {
      config.setInstanceZookeepers(host);
      try (FluoAdmin fluoAdmin = new FluoAdminImpl(config)) {
        fluoAdmin.initialize(opts);
        fail("This should have failed");
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }

  @Test
  public void testInitializeLongChroot() throws Exception {

    // stop oracle to avoid spurious exceptions when initializing
    oserver.stop();

    String zk = config.getAppZookeepers();
    String longPath = "/very/long/path";
    config.setInstanceZookeepers(zk + longPath);

    InitializationOptions opts = new InitializationOptions();
    opts.setClearZookeeper(true).setClearTable(true);

    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      admin.initialize(opts);
    }

    try (CuratorFramework curator = CuratorUtil.newRootFluoCurator(config)) {
      curator.start();
      Assert.assertNotNull(curator.checkExists().forPath(ZookeeperUtil.parseRoot(zk + longPath)));
    }

    String longPath2 = "/very/long/path2";
    config.setInstanceZookeepers(zk + longPath2);

    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      admin.initialize(opts);
    }

    try (CuratorFramework curator = CuratorUtil.newRootFluoCurator(config)) {
      curator.start();
      Assert.assertNotNull(curator.checkExists().forPath(ZookeeperUtil.parseRoot(zk + longPath2)));
      Assert.assertNotNull(curator.checkExists().forPath(ZookeeperUtil.parseRoot(zk + longPath)));
    }
  }

  @Test
  public void testRemove() throws Exception {

    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      admin.remove();
      fail("This should fail with the oracle server running");
    } catch (FluoException e) {
      // expected
    }

    // write/verify some data
    String row = "Logicians";
    Column fname = new Column("name", "first");
    Column lname = new Column("name", "last");

    try (FluoClient client = FluoFactory.newClient(config)) {
      try (Transaction tx = client.newTransaction()) {
        tx.set(row, fname, "Kurt");
        tx.set(row, lname, "Godel");
        tx.commit();
      }
      // read it for sanity
      try (Snapshot snap = client.newSnapshot()) {
        Assert.assertEquals("Kurt", snap.gets(row, fname));
        Assert.assertEquals("Godel", snap.gets(row, lname));
        Assert.assertEquals(2, Iterables.size(snap.scanner().build()));
      }
    }

    oserver.stop();

    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      admin.remove(); // pass with oracle stopped
    }

    try (FluoAdmin admin = new FluoAdminImpl(config)) {
      InitializationOptions opts =
          new InitializationOptions().setClearTable(false).setClearZookeeper(false);
      admin.initialize(opts);
    }

    // necessary workaround due to cached application id
    Environment env2 = new Environment(config);
    OracleServer oserver2 = new OracleServer(env2);
    oserver2.start();

    // verify empty
    try (FluoClient client = FluoFactory.newClient(config)) {
      try (Snapshot snap = client.newSnapshot()) {
        Assert.assertEquals(0, Iterables.size(snap.scanner().build()));
      }
    }

    try (FluoClient client = FluoFactory.newClient(config)) {
      // write data
      try (Transaction tx = client.newTransaction()) {
        tx.set(row, fname, "Stephen");
        tx.set(row, lname, "Kleene");
        tx.commit();
      }
      // read data
      try (Snapshot snap = client.newSnapshot()) {
        Assert.assertEquals("Stephen", snap.gets(row, fname));
        Assert.assertEquals("Kleene", snap.gets(row, lname));
        Assert.assertEquals(2, Iterables.size(snap.scanner().build()));
      }
    }

    oserver2.stop();
    env2.close();

  }

  @Test
  public void testNumOracles() throws Exception {
    try (FluoAdminImpl admin = new FluoAdminImpl(config)) {
      Assert.assertEquals(1, admin.numOracles());

      OracleServer oserver2 = new OracleServer(env);
      oserver2.start();
      oserver2.awaitLeaderElection(3, TimeUnit.SECONDS);
      Assert.assertEquals(2, admin.numOracles());

      oserver2.stop();
      oserver2.awaitLeaderElection(3, TimeUnit.SECONDS);
      Assert.assertEquals(1, admin.numOracles());

      oserver.stop();
      oserver.awaitLeaderElection(3, TimeUnit.SECONDS);
      Assert.assertEquals(0, admin.numOracles());
    }
  }

  @Test
  public void testNumOraclesWithMissingOraclePath() throws Exception {
    oserver.stop();
    try (CuratorFramework curator = CuratorUtil.newAppCurator(config);
        FluoAdminImpl admin = new FluoAdminImpl(config)) {
      curator.start();
      oserver.awaitLeaderElection(3, TimeUnit.SECONDS);
      curator.delete().forPath(ZookeeperPath.ORACLE_SERVER);

      assertEquals(0, admin.numOracles());
    }
  }

  @Test
  public void testOracleExists() throws Exception {
    try (FluoAdminImpl admin = new FluoAdminImpl(config)) {
      assertTrue(admin.oracleExists());

      oserver.stop();
      oserver.awaitLeaderElection(3, TimeUnit.SECONDS);

      assertFalse(admin.oracleExists());
    }
  }

  @Test
  public void testNumWorkers() {
    try (FluoAdminImpl admin = new FluoAdminImpl(config)) {
      assertEquals(0, admin.numWorkers());

      FluoWorker fluoWorker = new FluoWorkerImpl(config);
      fluoWorker.start();
      assertEquals(1, admin.numWorkers());

      FluoWorker fluoWorker2 = new FluoWorkerImpl(config);
      fluoWorker2.start();
      assertEquals(2, admin.numWorkers());

      fluoWorker2.stop();
      assertEquals(1, admin.numWorkers());

      fluoWorker.stop();
      assertEquals(0, admin.numWorkers());
    }
  }


  @Test
  public void testNumWorkersWithMissingWorkerPath() throws Exception {
    try (CuratorFramework curator = CuratorUtil.newAppCurator(config);
        FluoAdminImpl admin = new FluoAdminImpl(config)) {
      curator.start();
      if (curator.checkExists().forPath(ZookeeperPath.FINDERS) != null) {
        curator.delete().forPath(ZookeeperPath.FINDERS);
      }
      assertEquals(0, admin.numWorkers());
    }
  }
}
