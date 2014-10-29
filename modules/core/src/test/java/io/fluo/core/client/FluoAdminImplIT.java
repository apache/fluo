/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.core.client;

import io.fluo.accumulo.util.ZookeeperUtil;
import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoAdmin.AlreadyInitializedException;
import io.fluo.core.TestBaseImpl;
import io.fluo.core.util.CuratorUtil;
import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FluoAdminImplIT extends TestBaseImpl {

  @Test
  public void testInitializeTwiceFails() throws FluoAdmin.AlreadyInitializedException {

    FluoAdmin fluoAdmin = new FluoAdminImpl(config);

    config.setAllowReinitialize(true);

    fluoAdmin.initialize();
    fluoAdmin.initialize();

    config.setAllowReinitialize(false);
    try {
      fluoAdmin.initialize();
      fail("This should have failed");
    } catch(FluoAdmin.AlreadyInitializedException e) { }

    assertTrue(conn.tableOperations().exists(config.getAccumuloTable()));
  }

  @Test
  public void testInitializeWithNoChroot() throws AlreadyInitializedException {

    config.setAllowReinitialize(true);
    
    for (String host : new String[]{"localhost", "localhost/", "localhost:9999", "localhost:9999/"}) {
      config.setZookeepers(host);
      FluoAdmin fluoAdmin = new FluoAdminImpl(config);
      try {
        fluoAdmin.initialize();
        fail("This should have failed");
      } catch (IllegalArgumentException e) { }
    }
  }
  
  @Test
  public void testInitializeLongChroot() throws Exception {

    String zk = config.getZookeepers();
    String longPath = "/very/long/path";
    config.setAllowReinitialize(true);
    config.setZookeepers(zk + longPath);

    FluoAdmin fluoAdmin = new FluoAdminImpl(config);
    fluoAdmin.initialize();
    
    try (CuratorFramework curator = CuratorUtil.newRootFluoCurator(config)) {
      curator.start();
      Assert.assertNotNull(curator.checkExists().forPath(ZookeeperUtil.parseRoot(zk + longPath)));
    }
    
    String longPath2 = "/very/long/path2";
    config.setZookeepers(zk + longPath2);

    fluoAdmin = new FluoAdminImpl(config);
    fluoAdmin.initialize();
    
    try (CuratorFramework curator = CuratorUtil.newRootFluoCurator(config)) {
      curator.start();
      Assert.assertNotNull(curator.checkExists().forPath(ZookeeperUtil.parseRoot(zk + longPath2)));
      Assert.assertNotNull(curator.checkExists().forPath(ZookeeperUtil.parseRoot(zk + longPath)));
    }
  }
}
