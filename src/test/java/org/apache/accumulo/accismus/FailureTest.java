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

import org.apache.accumulo.accismus.Transaction.CommitData;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * 
 */
public class FailureTest {
  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  
  Column balanceCol = new Column("account", "balance");

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
  
  private void transfer(Connector conn, String from, String to, int amount) throws Exception {
    Transaction tx = new Transaction("bank", conn);
    
    int bal1 = Integer.parseInt(tx.get(from, balanceCol).toString());
    int bal2 = Integer.parseInt(tx.get(to, balanceCol).toString());
    
    tx.set(from, balanceCol, "" + (bal1 - amount));
    tx.set(to, balanceCol, "" + (bal2 + amount));
    
    tx.commit();
  }

  @Test
  public void testRollback() throws Exception {
    // test the case where a scan encounters a stuck lock and rolls it back
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    try {
      conn.tableOperations().delete("bank");
    } catch (TableNotFoundException e) {}
    
    Operations.createTable("bank", conn);
    
    Transaction tx = new Transaction("bank", conn);
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    
    tx.commit();
    
    Transaction tx2 = new Transaction("bank", conn);
    
    int bal1 = Integer.parseInt(tx2.get("bob", balanceCol).toString());
    int bal2 = Integer.parseInt(tx2.get("joe", balanceCol).toString());
    
    tx2.set("bob", balanceCol, "" + (bal1 - 7));
    tx2.set("joe", balanceCol, "" + (bal2 + 7));
    
    // get locks
    CommitData cd = tx2.preCommit();
    Assert.assertNotNull(cd);
    
    // TODO test rolling back primary and non-primary columns

    transfer(conn, "joe", "jill", 7);
    
    Transaction tx4 = new Transaction("bank", conn);
    
    Assert.assertEquals("10", tx4.get("bob", balanceCol).toString());
    Assert.assertEquals("13", tx4.get("joe", balanceCol).toString());
    Assert.assertEquals("67", tx4.get("jill", balanceCol).toString());
    
    long commitTs = Oracle.getInstance().getTimestamp();
    Assert.assertFalse(tx2.commitPrimaryColumn(cd, commitTs));
    
    transfer(conn, "bob", "joe", 7);
    
    Transaction tx6 = new Transaction("bank", conn);
    
    Assert.assertEquals("3", tx6.get("bob", balanceCol).toString());
    Assert.assertEquals("20", tx6.get("joe", balanceCol).toString());
    Assert.assertEquals("67", tx6.get("jill", balanceCol).toString());
  }

  @Test
  public void testRollfoward() throws Exception {
    // test the case where a scan encounters a stuck lock (for a complete tx) and rolls it forward
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    try {
      conn.tableOperations().delete("bank");
    } catch (TableNotFoundException e) {}
    
    Operations.createTable("bank", conn);
    
    Transaction tx = new Transaction("bank", conn);

    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    
    tx.commit();
    
    Transaction tx2 = new Transaction("bank", conn);
    
    int bal1 = Integer.parseInt(tx2.get("bob", balanceCol).toString());
    int bal2 = Integer.parseInt(tx2.get("joe", balanceCol).toString());
    
    tx2.set("bob", balanceCol, "" + (bal1 - 7));
    tx2.set("joe", balanceCol, "" + (bal2 + 7));
    
    // get locks
    CommitData cd = tx2.preCommit();
    Assert.assertNotNull(cd);
    long commitTs = Oracle.getInstance().getTimestamp();
    Assert.assertTrue(tx2.commitPrimaryColumn(cd, commitTs));

    // TODO test rolling forward primary and non-primary columns
    transfer(conn, "joe", "jill", 7);
    
    Transaction tx4 = new Transaction("bank", conn);
    
    Assert.assertEquals("3", tx4.get("bob", balanceCol).toString());
    Assert.assertEquals("20", tx4.get("joe", balanceCol).toString());
    Assert.assertEquals("67", tx4.get("jill", balanceCol).toString());

    tx2.finishCommit(cd, commitTs);
    
    Transaction tx5 = new Transaction("bank", conn);
    
    Assert.assertEquals("3", tx5.get("bob", balanceCol).toString());
    Assert.assertEquals("20", tx5.get("joe", balanceCol).toString());
    Assert.assertEquals("67", tx5.get("jill", balanceCol).toString());
  }
  
  @Test
  public void testAcks() {
    // TODO test that acks are properly handled in rollback and rollforward
  }
  
  @Test
  public void testStaleScan() throws Exception {
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    try {
      conn.tableOperations().delete("bank");
    } catch (TableNotFoundException e) {}
    
    Operations.createTable("bank", conn);
    
    Transaction tx = new Transaction("bank", conn);
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    
    tx.commit();
    
    Transaction tx2 = new Transaction("bank", conn);
    Assert.assertEquals("10", tx2.get("bob", balanceCol).toString());
    
    transfer(conn, "joe", "jill", 1);
    transfer(conn, "joe", "bob", 1);
    transfer(conn, "bob", "joe", 2);
    transfer(conn, "jill", "joe", 2);
    
    conn.tableOperations().flush("bank", null, null, true);
    
    try {
      tx2.get("joe", balanceCol);
      Assert.assertFalse(true);
    } catch (StaleScanException sse) {
      
    }
    
    Transaction tx3 = new Transaction("bank", conn);
    
    Assert.assertEquals("9", tx3.get("bob", balanceCol).toString());
    Assert.assertEquals("22", tx3.get("joe", balanceCol).toString());
    Assert.assertEquals("59", tx3.get("jill", balanceCol).toString());
  }
}
