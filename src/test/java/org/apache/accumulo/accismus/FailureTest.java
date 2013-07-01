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

import java.util.Random;

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
  public void testRollbackMany() throws Exception {
    // test writing lots of columns that need to be rolled back
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    try {
      conn.tableOperations().delete("trbm");
    } catch (TableNotFoundException e) {}
    
    Operations.createTable("trbm", conn);
    
    Column col1 = new Column("fam1", "q1");
    Column col2 = new Column("fam1", "q2");
    
    Transaction tx = new Transaction("trbm", conn);
    
    for (int r = 0; r < 10; r++) {
      tx.set(r + "", col1, "0" + r + "0");
      tx.set(r + "", col2, "0" + r + "1");
    }
    
    tx.commit();
    
    Transaction tx2 = new Transaction("trbm", conn);
    
    for (int r = 0; r < 10; r++) {
      tx2.set(r + "", col1, "1" + r + "0");
      tx2.set(r + "", col2, "1" + r + "1");
    }
    
    CommitData cd = tx2.preCommit();
    Assert.assertNotNull(cd);
    
    Transaction tx3 = new Transaction("trbm", conn);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("0" + r + "0", tx3.get(r + "", col1).toString());
      Assert.assertEquals("0" + r + "1", tx3.get(r + "", col2).toString());
    }
    
    long commitTs = Oracle.getInstance().getTimestamp();
    Assert.assertFalse(tx2.commitPrimaryColumn(cd, commitTs));
    
    Transaction tx4 = new Transaction("trbm", conn);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("0" + r + "0", tx4.get(r + "", col1).toString());
      Assert.assertEquals("0" + r + "1", tx4.get(r + "", col2).toString());
    }

  }

  @Test
  public void testRollforwardMany() throws Exception {
    // test writing lots of columns that need to be rolled forward
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    try {
      conn.tableOperations().delete("trfm");
    } catch (TableNotFoundException e) {}
    
    Operations.createTable("trfm", conn);
    
    Column col1 = new Column("fam1", "q1");
    Column col2 = new Column("fam1", "q2");
    
    Transaction tx = new Transaction("trfm", conn);
    
    for (int r = 0; r < 10; r++) {
      tx.set(r + "", col1, "0" + r + "0");
      tx.set(r + "", col2, "0" + r + "1");
    }
    
    tx.commit();
    
    Transaction tx2 = new Transaction("trfm", conn);
    
    for (int r = 0; r < 10; r++) {
      tx2.set(r + "", col1, "1" + r + "0");
      tx2.set(r + "", col2, "1" + r + "1");
    }
    
    CommitData cd = tx2.preCommit();
    Assert.assertNotNull(cd);
    long commitTs = Oracle.getInstance().getTimestamp();
    Assert.assertTrue(tx2.commitPrimaryColumn(cd, commitTs));
    
    Transaction tx3 = new Transaction("trfm", conn);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("1" + r + "0", tx3.get(r + "", col1).toString());
      Assert.assertEquals("1" + r + "1", tx3.get(r + "", col2).toString());
    }
    
    tx2.finishCommit(cd, commitTs);
    
    Transaction tx4 = new Transaction("trfm", conn);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("1" + r + "0", tx4.get(r + "", col1).toString());
      Assert.assertEquals("1" + r + "1", tx4.get(r + "", col2).toString());
    }
    
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
    
    // test rolling back primary and non-primary columns

    int bobBal = 10;
    int joeBal = 20;
    if ((new Random()).nextBoolean()) {
      transfer(conn, "joe", "jill", 7);
      joeBal -= 7;
    } else {
      transfer(conn, "bob", "jill", 7);
      bobBal -= 7;
    }
    
    Transaction tx4 = new Transaction("bank", conn);
    
    Assert.assertEquals(bobBal + "", tx4.get("bob", balanceCol).toString());
    Assert.assertEquals(joeBal + "", tx4.get("joe", balanceCol).toString());
    Assert.assertEquals("67", tx4.get("jill", balanceCol).toString());
    
    long commitTs = Oracle.getInstance().getTimestamp();
    Assert.assertFalse(tx2.commitPrimaryColumn(cd, commitTs));
    
    transfer(conn, "bob", "joe", 2);
    bobBal -= 2;
    joeBal += 2;
    
    Transaction tx6 = new Transaction("bank", conn);
    
    Assert.assertEquals(bobBal + "", tx6.get("bob", balanceCol).toString());
    Assert.assertEquals(joeBal + "", tx6.get("joe", balanceCol).toString());
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

    // test rolling forward primary and non-primary columns
    String bobBal = "3";
    String joeBal = "27";
    if ((new Random()).nextBoolean()) {
      transfer(conn, "joe", "jill", 2);
      joeBal = "25";
    } else {
      transfer(conn, "bob", "jill", 2);
      bobBal = "1";
    }
    
    Transaction tx4 = new Transaction("bank", conn);
    
    Assert.assertEquals(bobBal, tx4.get("bob", balanceCol).toString());
    Assert.assertEquals(joeBal, tx4.get("joe", balanceCol).toString());
    Assert.assertEquals("62", tx4.get("jill", balanceCol).toString());

    tx2.finishCommit(cd, commitTs);
    
    Transaction tx5 = new Transaction("bank", conn);
    
    Assert.assertEquals(bobBal, tx5.get("bob", balanceCol).toString());
    Assert.assertEquals(joeBal, tx5.get("joe", balanceCol).toString());
    Assert.assertEquals("62", tx5.get("jill", balanceCol).toString());
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
