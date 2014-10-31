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

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.accumulo.format.FluoFormatter;
import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.core.impl.Environment;
import io.fluo.core.oracle.OracleServer;
import io.fluo.core.util.CuratorUtil;
import io.fluo.core.util.PortUtils;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloInstance;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * Base Integration Test exposing underlying implementation
 */
public class TestBaseImpl {
  protected final static String USER = "root";
  protected final static String PASSWORD = "ITSecret";
  protected static AtomicInteger next = new AtomicInteger();
  protected static Instance instance;
  protected static CuratorFramework curator;
  protected static Connector conn;

  protected Environment env;
  protected String table;
  protected OracleServer oserver;
  protected FluoConfiguration config;
  protected FluoClient client;
  
  protected class TestOracle extends OracleServer implements AutoCloseable {

    Environment env;

    TestOracle(Environment env) throws Exception {
      super(env);
      this.env = env;
    }

    @SuppressWarnings("resource")
    TestOracle(int port) throws Exception {
      this(new Environment(config, curator, conn, port));
    }

    @Override
    public void close() throws Exception {
      env.close();
    }
  }

  protected List<ObserverConfiguration> getObservers() {
    return Collections.emptyList();
  }

  @BeforeClass
  public static void setUp() throws Exception {
    String instanceName = "plugin-it-instance";
    instance = new MiniAccumuloInstance(instanceName, new File("target/accumulo-maven-plugin/" + instanceName));
    conn = instance.getConnector(USER, new PasswordToken(PASSWORD));
  }

  @Before
  public void setup() throws Exception {

    table = "table" + next.getAndIncrement();
    String zkRoot = "/impl-test" + next.getAndIncrement();

    config = new FluoConfiguration();
    config.setAccumuloInstance(instance.getInstanceName());
    config.setAccumuloUser(USER);
    config.setAccumuloPassword(PASSWORD);
    config.setAccumuloTable(table);
    config.setAccumuloZookeepers(instance.getZooKeepers());
    config.setZookeepers(instance.getZooKeepers() + zkRoot);
    config.setTransactionRollbackTime(1, TimeUnit.SECONDS);
    config.setObservers(getObservers());
    
    curator = CuratorUtil.newFluoCurator(config);
    curator.start();
    
    FluoAdmin admin = FluoFactory.newAdmin(config);
    admin.initialize();
   
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
  public void tearDown() throws Exception {
    conn.tableOperations().delete(table);
    if(oserver.isConnected())
      oserver.stop();
    env.close();
    client.close();
    curator.close();
  }

  protected void printTable() throws Exception {
    Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
    FluoFormatter af = new FluoFormatter();

    af.initialize(scanner, true);

    while (af.hasNext()) {
      System.out.println(af.next());
    }
  }
}
