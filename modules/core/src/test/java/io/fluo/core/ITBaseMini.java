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
package io.fluo.core;

import java.util.concurrent.TimeUnit;

import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoAdmin.InitOpts;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.mini.MiniFluo;
import io.fluo.core.util.PortUtils;
import org.apache.commons.configuration.Configuration;
import org.junit.After;
import org.junit.Before;

/** 
 * Base Integration Test class implemented using MiniFluo
 */
public class ITBaseMini extends ITBase {
  
  protected static MiniFluo miniFluo;

  protected void setAppConfig(Configuration config) {
  }
  
  @Before
  public void setUpFluo() throws Exception {
    String zkRoot = "/mini-test" + testCounter.getAndIncrement();
    
    config = new FluoConfiguration();
    config.setAccumuloInstance(miniAccumulo.getInstanceName());
    config.setAccumuloUser(USER);
    config.setAccumuloPassword(PASSWORD);
    config.setAccumuloZookeepers(miniAccumulo.getZooKeepers());
    config.setZookeepers(miniAccumulo.getZooKeepers() + zkRoot);
    config.setAccumuloTable(getNextTableName());
    config.setWorkerThreads(5);
    config.setObservers(getObservers());
    config.setOraclePort(PortUtils.getRandomFreePort());
    config.setMiniStartAccumulo(false);
  
    setAppConfig(config.getAppConfiguration());
    
    config.setTransactionRollbackTime(1, TimeUnit.SECONDS);
    
    FluoAdmin admin = FluoFactory.newAdmin(config);
    InitOpts opts = new InitOpts().setClearZookeeper(true).setClearTable(true);
    admin.initialize(opts);
   
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
