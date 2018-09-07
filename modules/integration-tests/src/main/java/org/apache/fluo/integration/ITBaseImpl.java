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
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.oracle.OracleServer;
import org.junit.After;
import org.junit.Before;

/**
 * Base Integration Test exposing underlying implementation
 */
public class ITBaseImpl extends ITBase {

  protected Environment env;
  protected String table;
  protected OracleServer oserver;

  private static Environment initOracleEnv(FluoConfiguration config, int port) {
    FluoConfiguration c = new FluoConfiguration(config);
    c.setProperty(FluoConfigurationImpl.ORACLE_PORT_PROP, port);
    return new Environment(c);
  }

  protected class TestOracle extends OracleServer implements AutoCloseable {

    private Environment env;

    TestOracle(Environment env) throws Exception {
      super(env);
      this.env = env;
    }

    TestOracle(int port) throws Exception {
      this(initOracleEnv(config, port));
    }

    @Override
    public void close() {
      env.close();
    }
  }

  @Before
  public void setUpFluo() throws Exception {

    table = getNextTableName();

    config = new FluoConfiguration();
    config.setApplicationName("impl-test" + testCounter.getAndIncrement());
    config.setAccumuloInstance(clientInfo.getInstanceName());
    config.setAccumuloUser(USER);
    config.setAccumuloPassword(PASSWORD);
    config.setAccumuloTable(table);
    config.setAccumuloZookeepers(clientInfo.getZooKeepers());
    config.setInstanceZookeepers(clientInfo.getZooKeepers() + "/fluo");
    config.setTransactionRollbackTime(1, TimeUnit.SECONDS);
    setupObservers(config);
    config.setProperty(FluoConfigurationImpl.ZK_UPDATE_PERIOD_PROP, "1000");
    config.setMiniStartAccumulo(false);

    try (FluoAdmin admin = FluoFactory.newAdmin(config)) {
      InitializationOptions opts =
          new InitializationOptions().setClearZookeeper(true).setClearTable(true);
      admin.initialize(opts);
    }

    client = FluoFactory.newClient(config);

    env = new Environment(config);

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
    client.close();
    if (oserver.isConnected()) {
      oserver.stop();
    }
    env.close();
    aClient.tableOperations().delete(table);
  }
}
