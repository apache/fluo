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
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.accumulo.format.FluoFormatter;
import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.MiniFluo;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.core.util.PortUtils;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

/** 
 * Base Integration Test Class that uses MiniFluoImpl
 */
public class TestBaseMini {
  
  protected static Instance miniAccumulo;
  protected static FluoConfiguration config;
  protected static MiniFluo miniFluo;
  protected static AtomicInteger tableCounter = new AtomicInteger(1);
  protected static AtomicInteger next = new AtomicInteger();
  protected static String USER = "root";
  protected static String PASSWORD = "ITSecret";
  protected static FluoClient client;
  
  @BeforeClass
  public static void setUpAccumulo() throws FileNotFoundException {
    String instanceName = "plugin-it-instance";
    miniAccumulo = new MiniAccumuloInstance(instanceName, new File("target/accumulo-maven-plugin/" + instanceName));
  }
  
  public String getCurrTableName() {
    return "data" + tableCounter.get();
  }
  
  public String getNextTableName() {
    return "data" + tableCounter.incrementAndGet();
  }
  
  protected List<ObserverConfiguration> getObservers() {
    return Collections.emptyList();
  }

  @Before
  public void setUpFluo() throws Exception {
    // TODO add helper code to make this shorter
    
    config = new FluoConfiguration();
    config.setAccumuloInstance(miniAccumulo.getInstanceName());
    config.setAccumuloUser(USER);
    config.setAccumuloPassword(PASSWORD);
    config.setZookeeperRoot("/stress" + next.getAndIncrement());
    config.setZookeepers(miniAccumulo.getZooKeepers());
    config.setClearZookeeper(true);
    config.setAccumuloTable(getNextTableName());
    config.setWorkerThreads(5);
    config.setObservers(getObservers());
    config.setOraclePort(PortUtils.getRandomFreePort());
  
    FluoAdmin admin = FluoFactory.newAdmin(config);
    admin.initialize();
   
    client = FluoFactory.newClient(config);

    miniFluo = FluoFactory.newMiniFluo(config);
    miniFluo.start();
  }
  
  @After
  public void tearDownFluo() throws Exception {
    miniFluo.stop();
    client.close();
  }
  
  @SuppressWarnings("deprecation")
  protected void printTable() throws Exception {
    Scanner scanner = miniAccumulo.getConnector(USER, PASSWORD).createScanner(getCurrTableName(), Authorizations.EMPTY);
    FluoFormatter af = new FluoFormatter();

    af.initialize(scanner, true);
    while (af.hasNext()) {
      System.out.println(af.next());
    }
  }
  
  protected void printSnapshot() throws Exception {

    Snapshot s = client.newSnapshot();
    RowIterator iter = s.get(new ScannerConfiguration());

    System.out.println("== snapshot start ==");
    while (iter.hasNext()) {
      Entry<Bytes, ColumnIterator> rowEntry = iter.next();
      ColumnIterator citer = rowEntry.getValue();
      while (citer.hasNext()) {
        Entry<Column, Bytes> colEntry = citer.next();
        System.out.println(rowEntry.getKey()+" "+colEntry.getKey()+"\t"+colEntry.getValue());
      }
    }  
    System.out.println("=== snapshot end ===");
  }
}
