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
package accismus.stress;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import accismus.api.Admin;
import accismus.api.Column;
import accismus.api.ColumnIterator;
import accismus.api.RowIterator;
import accismus.api.ScannerConfiguration;
import accismus.api.Snapshot;
import accismus.api.SnapshotFactory;
import accismus.api.config.InitializationProperties;
import accismus.api.config.ObserverConfiguration;
import accismus.api.test.MiniAccismus;
import accismus.format.AccismusFormatter;

/** Base class used to build stress test ITs 
 * */
public class StressBase {
  
  protected static Instance miniAccumulo;
  protected static InitializationProperties props;
  protected static MiniAccismus miniAccismus;
  protected static AtomicInteger tableCounter = new AtomicInteger(1);
  protected static String USER = "root";
  protected static String PASSWORD = "ITSecret";
  
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
  public void setUpAccismus() throws Exception {
    // TODO add helper code to make this shorter
    props = new InitializationProperties();
    props.setAccumuloInstance(miniAccumulo.getInstanceName());
    props.setAccumuloUser(USER);
    props.setAccumuloPassword(PASSWORD);
    props.setZookeeperRoot("/accismus");
    props.setZookeepers(miniAccumulo.getZooKeepers());
    props.setClearZookeeper(true);
    props.setAccumuloTable(getNextTableName());
    props.setNumThreads(5);
    props.setObservers(getObservers());
  
    Admin.initialize(props);

    miniAccismus = new MiniAccismus(props);
    miniAccismus.start();
  }
  
  @After
  public void tearDownAccismus() throws Exception {
    miniAccismus.stop();
  }
  
  @SuppressWarnings("deprecation")
  protected void printTable() throws Exception {
    Scanner scanner = miniAccumulo.getConnector(USER, PASSWORD).createScanner(getCurrTableName(), Authorizations.EMPTY);
    AccismusFormatter af = new AccismusFormatter();

    af.initialize(scanner, true);
    while (af.hasNext()) {
      System.out.println(af.next());
    }
  }
  
  protected void printSnapshot() throws Exception {
    try (SnapshotFactory snapFact = new SnapshotFactory(props)) {

      Snapshot s = snapFact.createSnapshot();

      ScannerConfiguration scannerConf = new ScannerConfiguration();

      RowIterator iter = s.get(scannerConf);

      System.out.println("== snapshot start ==");
      while (iter.hasNext()) {
        Entry<ByteSequence, ColumnIterator> rowEntry = iter.next();
        ColumnIterator citer = rowEntry.getValue();
        while (citer.hasNext()) {
          Entry<Column, ByteSequence> colEntry = citer.next();
          System.out.println(rowEntry.getKey()+" "+colEntry.getKey()+"\t"+colEntry.getValue());
        }
      }  
      System.out.println("=== snapshot end ===");
    }
  }
}
