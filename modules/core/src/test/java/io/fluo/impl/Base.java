/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.impl;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.api.Admin;
import io.fluo.api.Column;
import io.fluo.api.Observer;
import io.fluo.api.config.InitializationProperties;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.core.util.PortUtils;
import io.fluo.format.FluoFormatter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloInstance;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * 
 */
public class Base {
  protected static String secret = "ITSecret";
  protected static ZooKeeper zk;
  protected static AtomicInteger next = new AtomicInteger();
  protected static Instance instance;
  protected static CuratorFramework curator;

  protected Configuration config;
  protected Connector conn;
  protected String table;
  protected OracleServer oserver;
  protected String zkn;
  protected InitializationProperties props;
  
  protected List<ObserverConfiguration> getObservers() {
    return Collections.emptyList();
  }

  protected void runWorker() throws Exception, TableNotFoundException {
    // TODO pass a tablet chooser that returns first tablet
    Worker worker = new Worker(config, new RandomTabletChooser(config));
    Map<Column,Observer> colObservers = new HashMap<Column,Observer>();
    try {
      while (true) {
        worker.processUpdates(colObservers);

        // there should not be any notifcations
        Scanner scanner = conn.createScanner(table, new Authorizations());
        scanner.fetchColumnFamily(ByteUtil.toText(Constants.NOTIFY_CF));

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
    zk = new ZooKeeper(instance.getZooKeepers(), 30000, null);
  }

  @Before
  public void setup() throws Exception {

    conn = instance.getConnector("root", new PasswordToken(secret));

    table = "table" + next.getAndIncrement();
    zkn = "/test" + next.getAndIncrement();

    props = new InitializationProperties();

    props.setAccumuloInstance(instance.getInstanceName());
    props.setAccumuloUser("root");
    props.setAccumuloPassword(secret);
    props.setAccumuloTable(table);
    props.setZookeeperRoot(zkn);
    props.setZookeepers(instance.getZooKeepers());
    props.setRollbackTime(1, TimeUnit.SECONDS);
    props.setObservers(getObservers());

    Admin.initialize(props);
    
    config = new Configuration(zk, zkn, conn, PortUtils.getRandomFreePort());
    curator = config.getSharedResources().getCurator();
    
    oserver = new OracleServer(config);
    oserver.start();
  }

  /** Utility method to create additional oracles (setup method 
   * will always create one oracle)
   */
  public OracleServer createExtraOracle(int port) throws Exception {
    Configuration config = new Configuration(zk, zkn, conn, port);
    return new OracleServer(config);
  }
  
  @After
  public void tearDown() throws Exception {
    conn.tableOperations().delete(table);
    if(oserver.isConnected())
      oserver.stop();

    config.getSharedResources().close();
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
