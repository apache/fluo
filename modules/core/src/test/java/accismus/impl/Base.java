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
package accismus.impl;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloInstance;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import accismus.api.Column;
import accismus.api.Observer;
import accismus.api.config.ObserverConfiguration;
import accismus.api.config.OracleProperties;
import accismus.api.config.TransactionConfiguration;
import accismus.format.AccismusFormatter;

/**
 * 
 */
public class Base {
  protected static String secret = "ITSecret";
  
  protected static ZooKeeper zk;
  
  protected static final Map<Column,ObserverConfiguration> EMPTY_OBSERVERS = new HashMap<Column,ObserverConfiguration>();
  
  protected static AtomicInteger next = new AtomicInteger();
  
  protected static Instance instance;
  
  protected Configuration config;
  protected Connector conn;
  protected String table;
  protected OracleServer oserver;
  protected String zkn;

  protected Map<Column,ObserverConfiguration> getObservers() {
    return EMPTY_OBSERVERS;
  }

  protected Map<Column,ObserverConfiguration> getWeakObservers() {
    return EMPTY_OBSERVERS;
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
    
    Operations.initialize(conn, zkn, table);
    Properties wprops = new Properties();
    wprops.setProperty(TransactionConfiguration.ROLLBACK_TIME_PROP, "5000");
    Operations.updateWorkerConfig(conn, zkn, wprops);
    Operations.updateObservers(conn, zkn, getObservers(), getWeakObservers());

    config = new Configuration(zk, zkn, conn, OracleProperties.ORACLE_DEFAULT_PORT);
    
    oserver = createOracle(9913);
    oserver.start();
  }

  public OracleServer createOracle(int port) throws Exception {
    config = new Configuration(zk, zkn, conn, port);
    return new OracleServer(config);
  }
  
  @After
  public void tearDown() throws Exception {
    conn.tableOperations().delete(table);
    oserver.stop();
    config.getSharedResources().close();
  }

  protected void printTable() throws Exception {
    Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
    AccismusFormatter af = new AccismusFormatter();

    af.initialize(scanner, true);

    while (af.hasNext()) {
      System.out.println(af.next());
    }
  }
}
