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
package org.apache.accumulo.accismus.impl;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.accismus.api.Column;
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

/**
 * 
 */
public class Base {
  protected static String secret = "ITSecret";
  
  protected static ZooKeeper zk;
  
  protected static final Map<Column,String> EMPTY_OBSERVERS = new HashMap<Column,String>();
  
  protected static AtomicInteger next = new AtomicInteger();
  
  private static Instance instance;
  
  protected Configuration config;
  protected Connector conn;
  protected String table;
  protected OracleServer oserver;
  protected String zkn;

  protected Map<Column,String> getObservers() {
    return EMPTY_OBSERVERS;
  }

  protected void runWorker() throws Exception, TableNotFoundException {
    // TODO pass a tablet chooser that returns first tablet
    Worker worker = new Worker(config, new RandomTabletChooser(config));
    while (true) {
      worker.processUpdates();
      
      // there should not be any notifcations
      Scanner scanner = conn.createScanner(table, new Authorizations());
      scanner.fetchColumnFamily(ByteUtil.toText(Constants.NOTIFY_CF));
      
      if (!scanner.iterator().hasNext())
        break;
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
    Operations.updateWorkerConfig(conn, zkn, new Properties());
    Operations.updateObservers(conn, zkn, getObservers());

    config = new Configuration(zk, zkn, conn);
    
    oserver = new OracleServer(config);
    oserver.start();
  }
  
  @After
  public void tearDown() throws Exception {
    conn.tableOperations().delete(table);
    oserver.stop();
  }
}
