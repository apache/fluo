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
import io.fluo.core.impl.Environment;
import io.fluo.core.oracle.OracleServer;
import io.fluo.core.util.CuratorUtil;
import io.fluo.core.util.PortUtils;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;

/**
 * Base Integration Test exposing underlying implementation
 */
public class ITBaseImpl extends ITBase {

  protected static CuratorFramework curator;
  protected Environment env;
  protected String table;
  protected OracleServer oserver;
  
  protected class TestOracle extends OracleServer implements AutoCloseable {

    Environment env;

    TestOracle(Environment env) throws Exception {
      super(env);
      this.env = env;
    }

    TestOracle(int port) throws Exception {
      this(new Environment(config, curator, conn, port));
    }

    @Override
    public void close() throws Exception {
      env.close();
    }
  }

  @Before
  public void setUpFluo() throws Exception {

    table = getNextTableName();
    String zkRoot = "/impl-test" + testCounter.getAndIncrement();

    config = new FluoConfiguration();
    config.setAccumuloInstance(miniAccumulo.getInstanceName());
    config.setAccumuloUser(USER);
    config.setAccumuloPassword(PASSWORD);
    config.setAccumuloTable(table);
    config.setAccumuloZookeepers(miniAccumulo.getZooKeepers());
    config.setZookeepers(miniAccumulo.getZooKeepers() + zkRoot);
    config.setTransactionRollbackTime(1, TimeUnit.SECONDS);
    config.setObservers(getObservers());
    
    curator = CuratorUtil.newFluoCurator(config);
    curator.start();
    
    FluoAdmin admin = FluoFactory.newAdmin(config);
    InitOpts opts = new InitOpts().setClearZookeeper(true).setClearTable(true);
    admin.initialize(opts);
   
    client = FluoFactory.newClient(config);

    env = new Environment(config, curator, conn, PortUtils.getRandomFreePort());
    
    oserver = new OracleServer(env);
    oserver.start();
  }

  /**
   * Utility method to create additional oracles (setup method will always create one oracle)
   */
  public TestOracle createExtraOracle(int port) throws Exception {
    return new TestOracle(port);
  }
  
  @After
  public void tearDownFluo() throws Exception {
    conn.tableOperations().delete(table);
    if (oserver.isConnected())
      oserver.stop();
    env.close();
    client.close();
    curator.close();
  }
}
