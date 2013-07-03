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
package org.apache.accumulo.accismus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.accismus.impl.OracleServer;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.rules.TemporaryFolder;

/**
 * 
 */
public class TestBase {
  protected static String secret = "superSecret";
  protected static TemporaryFolder folder = new TemporaryFolder();
  protected static MiniAccumuloCluster cluster;
  protected static ZooKeeper zk;
  
  protected static final Map<Column,Class<? extends Observer>> EMPTY_OBSERVERS = new HashMap<Column,Class<? extends Observer>>();
  
  protected static AtomicInteger next = new AtomicInteger();
  
  protected Configuration config;
  protected Connector conn;
  protected String table;
  protected OracleServer oserver;
  protected String zkn;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), secret);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
    
    zk = new ZooKeeper(cluster.getZooKeepers(), 30000, null);
  }
  
  @Before
  public void setup() throws Exception {
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    table = "table" + next.getAndIncrement();
    zkn = "/test" + next.getAndIncrement();
    
    Operations.initialize(conn, zkn, table, EMPTY_OBSERVERS);
    config = new Configuration(zk, zkn, conn);
    
    oserver = new OracleServer(config);
    oserver.start();
  }
  
  @After
  public void tearDown() throws Exception {
    conn.tableOperations().delete(table);
    oserver.stop();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }
}
