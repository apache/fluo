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

import org.apache.accumulo.accismus.format.AccismusFormatter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * 
 */
public class WorkerTest {
  
  private static final ByteSequence NODE_CF = new ArrayByteSequence("node");

  static class DegreeIndexer implements Observer {
    
    public void process(Transaction tx, ByteSequence row, Column col) throws Exception {
      // get previously calculated degree
      
      ByteSequence degree = tx.get(row, new Column("attr", "degree"));

      // calculate new degree
      int count = 0;
      RowIterator riter = tx.get(new ScannerConfiguration().setRange(Range.exact(new Text(row.toArray()), new Text("link"))));
      while (riter.hasNext()) {
        ColumnIterator citer = riter.next().getValue();
        while (citer.hasNext()) {
          citer.next();
          count++;
        }
      }
      String degree2 = "" + count;
      
      if (degree == null || !degree.toString().equals(degree2)) {
        tx.set(row, new Column("attr", "degree"), new ArrayByteSequence(degree2));
        
        // put new entry in degree index
        tx.set("IDEG" + degree2, new Column(NODE_CF, row), "");
      }
      
      if (degree != null) {
        // delete old degree in index
        tx.delete("IDEG" + degree, new Column(NODE_CF, row));
      }
      
      // TODO maybe commit should be done externally
      System.out.println("commit " + tx.commit());
    }
  }
  
  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), secret);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }
  
  @Test
  public void test1() throws Exception {
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    Operations.createTable("graph", conn);
    
    ColumnSet observed = new ColumnSet();
    observed.add(new Column("attr", "lastupdate"));
    
    Transaction tx1 = new Transaction("graph", conn, observed);
    
    tx1.set("N0003", new Column("link", "N0040"), "");
    tx1.set("N0003", new Column("attr", "lastupdate"), System.currentTimeMillis() + "");
    
    Assert.assertTrue(tx1.commit());
    
    Transaction tx2 = new Transaction("graph", conn, observed);
    
    tx2.set("N0003", new Column("link", "N0020"), "");
    tx2.set("N0003", new Column("attr", "lastupdate"), System.currentTimeMillis() + "");
    
    Assert.assertTrue(tx2.commit());
    
    Map<Column,Observer> observers = new HashMap<Column,Observer>();
    observers.put(new Column("attr", "lastupdate"), new DegreeIndexer());
    
    Worker worker = new Worker("graph", conn, observers);
    
    worker.processUpdates();
    
    Transaction tx3 = new Transaction("graph", conn);
    Assert.assertEquals("2", tx3.get("N0003", new Column("attr", "degree")).toString());
    Assert.assertEquals("", tx3.get("IDEG2", new Column("node", "N0003")).toString());
    
    tx3.set("N0003", new Column("link", "N0010"), "");
    tx3.set("N0003", new Column("attr", "lastupdate"), System.currentTimeMillis() + "");
    Assert.assertTrue(tx3.commit());
    
    worker.processUpdates();
    
    Transaction tx4 = new Transaction("graph", conn);
    Assert.assertEquals("3", tx4.get("N0003", new Column("attr", "degree")).toString());
    Assert.assertNull("", tx4.get("IDEG2", new Column("node", "N0003")));
    Assert.assertEquals("", tx4.get("IDEG3", new Column("node", "N0003")).toString());
    
    // TODO remove these prints
    Scanner scanner = conn.createScanner("graph", new Authorizations());
    AccismusFormatter formatter = new AccismusFormatter();
    formatter.initialize(scanner, true);
    while (formatter.hasNext()) {
      System.out.println(formatter.next());
    }
    
    conn.tableOperations().flush("graph", null, null, true);

    System.out.println();
    
    formatter.initialize(scanner, true);
    while (formatter.hasNext()) {
      System.out.println(formatter.next());
    }

  }
}
