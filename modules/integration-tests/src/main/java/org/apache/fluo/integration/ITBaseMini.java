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

package org.apache.fluo.integration;

import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoAdmin.InitializationOptions;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.mini.MiniFluo;
import org.junit.After;
import org.junit.Before;

/**
 * Base Integration Test class implemented using MiniFluo
 */
public class ITBaseMini extends ITBase {

  protected static MiniFluo miniFluo;

  protected void setAppConfig(SimpleConfiguration config) {}

  protected void setConfig(FluoConfiguration config) {}

  @Before
  public void setUpFluo() throws Exception {

    config = new FluoConfiguration();
    config.setApplicationName("mini-test" + testCounter.getAndIncrement());
    config.setAccumuloInstance(miniAccumulo.getInstanceName());
    config.setAccumuloUser(USER);
    config.setAccumuloPassword(PASSWORD);
    config.setAccumuloZookeepers(miniAccumulo.getZooKeepers());
    config.setInstanceZookeepers(miniAccumulo.getZooKeepers() + "/fluo");
    config.setAccumuloTable(getNextTableName());
    config.setWorkerThreads(5);
    setupObservers(config);
    config.setMiniStartAccumulo(false);

    setConfig(config);
    setAppConfig(config.getAppConfiguration());

    config.setTransactionRollbackTime(1, TimeUnit.SECONDS);

    try (FluoAdmin admin = FluoFactory.newAdmin(config)) {
      InitializationOptions opts =
          new InitializationOptions().setClearZookeeper(true).setClearTable(true);
      admin.initialize(opts);
    }

    config.getAppConfiguration().clear();

    client = FluoFactory.newClient(config);
    miniFluo = FluoFactory.newMiniFluo(config);
  }

  @After
  public void tearDownFluo() throws Exception {
    miniFluo.close();
    client.close();
  }
}
