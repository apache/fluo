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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.accumulo.format.FluoFormatter;
import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Column;
import io.fluo.api.observer.Observer;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.RandomTabletChooser;
import io.fluo.core.impl.Worker;
import io.fluo.core.oracle.OracleServer;
import io.fluo.core.util.ByteUtil;
import io.fluo.core.util.CuratorUtil;
import io.fluo.core.util.PortUtils;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
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
  protected static String secret = "ITSecret";
  protected static AtomicInteger next = new AtomicInteger();
  protected static Instance instance;
  protected static CuratorFramework curator;
  protected static Connector conn;

  protected Environment env;
  protected String table;
  protected OracleServer oserver;
  protected String zkn;
  protected FluoConfiguration config;
  protected FluoClient client;
  
  protected List<ObserverConfiguration> getObservers() {
    return Collections.emptyList();
  }

  protected void runWorker() throws Exception, TableNotFoundException {
    // TODO pass a tablet chooser that returns first tablet
    Worker worker = new Worker(env, new RandomTabletChooser(env));
    Map<Column,Observer> colObservers = new HashMap<Column,Observer>();
    try {
      while (true) {
        worker.processUpdates(colObservers);

        // there should not be any notifcations
        Scanner scanner = conn.createScanner(table, new Authorizations());
        scanner.fetchColumnFamily(ByteUtil.toText(ColumnConstants.NOTIFY_CF));

        if (!scanner.iterator().hasNext())
          break;
      }
    } finally {
      for (Observer observer : colObservers.values()) {
        try {
          observer.close();
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    String instanceName = "plugin-it-instance";
    instance = new MiniAccumuloInstance(instanceName, new File("target/accumulo-maven-plugin/" + instanceName));
    conn = instance.getConnector("root", new PasswordToken(secret));
    curator = CuratorUtil.getCurator(conn.getInstance().getZooKeepers(), 30000);
    curator.start();
  }

  @Before
  public void setup() throws Exception {

    table = "table" + next.getAndIncrement();
    zkn = "/test" + next.getAndIncrement();

    config = new FluoConfiguration();
    config.setAccumuloInstance(instance.getInstanceName());
    config.setAccumuloUser("root");
    config.setAccumuloPassword(secret);
    config.setAccumuloTable(table);
    config.setZookeeperRoot(zkn);
    config.setZookeepers(instance.getZooKeepers());
    config.setTransactionRollbackTime(1, TimeUnit.SECONDS);
    config.setObservers(getObservers());
    
    FluoAdmin admin = FluoFactory.newAdmin(config);
    admin.initialize();
   
    client = FluoFactory.newClient(config);

    env = new Environment(curator, zkn, conn, PortUtils.getRandomFreePort());
    
    oserver = new OracleServer(env);
    oserver.start();
  }

  /** Utility method to create additional oracles (setup method 
   * will always create one oracle)
   */
  public OracleServer createExtraOracle(int port) throws Exception {
    Environment env = new Environment(curator, zkn, conn, port);
    return new OracleServer(env);
  }
  
  @After
  public void tearDown() throws Exception {
    conn.tableOperations().delete(table);
    if(oserver.isConnected())
      oserver.stop();
    env.close();
    client.close();
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
