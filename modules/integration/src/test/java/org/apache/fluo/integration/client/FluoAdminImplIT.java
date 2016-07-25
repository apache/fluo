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

import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.accumulo.util.ZookeeperUtil;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoAdmin.AlreadyInitializedException;
import org.apache.fluo.api.client.FluoAdmin.InitializationOptions;
import org.apache.fluo.api.client.FluoAdmin.TableExistsException;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.fluo.integration.ITBaseImpl;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FluoAdminImplIT extends ITBaseImpl {

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
      }

      opts.setClearZookeeper(false).setClearTable(true);
      try {
        admin.initialize(opts);
        fail("This should have failed");
      } catch (AlreadyInitializedException e) {
      }

      opts.setClearZookeeper(true).setClearTable(false);
      try {
        admin.initialize(opts);
        fail("This should have failed");
      } catch (TableExistsException e) {
      }
    }

    assertTrue(conn.tableOperations().exists(config.getAccumuloTable()));
  }

  @Test
  public void testInitializeWithNoChroot() throws Exception {

    // stop oracle to avoid spurious exceptions when initializing
    oserver.stop();

    InitializationOptions opts =
        new InitializationOptions().setClearZookeeper(true).setClearTable(true);

    for (String host : new String[] {"localhost", "localhost/", "localhost:9999", "localhost:9999/"}) {
      config.setInstanceZookeepers(host);
      try (FluoAdmin fluoAdmin = new FluoAdminImpl(config)) {
        fluoAdmin.initialize(opts);
        fail("This should have failed");
      } catch (IllegalArgumentException e) {
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
}
