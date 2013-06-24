package org.apache.accumulo.accismus;

import java.util.HashSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AccismusTest {
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
  
  @Test
  public void testOverlap1() throws Exception {
    // test transactions that overlap reads and both attempt to write
    // TX1 starts
    // TX2 starts
    // TX1 reads/writes
    // TX2 reads/writes
    // TX2 commits -- succeeds
    // TX1 commits -- fails
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    Operations.createTable("bank", conn);
    
    Transaction tx = new Transaction("bank", conn);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    
    Assert.assertTrue(tx.commit());
    
    tx = new Transaction("bank", conn);
    
    String bal1 = tx.get("bob", balanceCol).toString();
    String bal2 = tx.get("joe", balanceCol).toString();
    Assert.assertEquals("10", bal1);
    Assert.assertEquals("20", bal2);
    
    tx.set("bob", balanceCol, (Long.parseLong(bal1) - 5) + "");
    tx.set("joe", balanceCol, (Long.parseLong(bal2) + 5) + "");
    
    Transaction tx2 = new Transaction("bank", conn);
    
    String bal3 = tx2.get("bob", balanceCol).toString();
    String bal4 = tx2.get("jill", balanceCol).toString();
    Assert.assertEquals("10", bal3);
    Assert.assertEquals("60", bal4);
    
    tx2.set("bob", balanceCol, (Long.parseLong(bal3) - 5) + "");
    tx2.set("jill", balanceCol, (Long.parseLong(bal4) + 5) + "");
    
    Assert.assertTrue(tx2.commit());
    Assert.assertFalse(tx.commit());
    
    Transaction tx3 = new Transaction("bank", conn);
    
    Assert.assertEquals("5", tx3.get("bob", balanceCol).toString());
    Assert.assertEquals("20", tx3.get("joe", balanceCol).toString());
    Assert.assertEquals("65", tx3.get("jill", balanceCol).toString());
  }
  
  @Test
  public void testSnapshots() throws Exception {
    // test the following case
    // TX1 starts
    // TX2 starts
    // TX2 reads/writes
    // TX2 commits
    // TX1 reads -- should not see TX2 writes
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    Operations.createTable("bank2", conn);
    
    Transaction tx = new Transaction("bank2", conn);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    tx.set("jane", balanceCol, "0");
    
    Assert.assertTrue(tx.commit());
    
    Transaction tx1 = new Transaction("bank2", conn);
    
    Transaction tx2 = new Transaction("bank2", conn);
    tx2.set("bob", balanceCol, (Long.parseLong(tx2.get("bob", balanceCol).toString()) - 5) + "");
    tx2.set("joe", balanceCol, (Long.parseLong(tx2.get("joe", balanceCol).toString()) - 5) + "");
    tx2.set("jill", balanceCol, (Long.parseLong(tx2.get("jill", balanceCol).toString()) + 10) + "");
    
    String bal1 = tx1.get("bob", balanceCol).toString();
    
    Assert.assertTrue(tx2.commit());
    
    Transaction txd = new Transaction("bank2", conn);
    txd.delete("jane", balanceCol);
    txd.commit();
    
    String bal2 = tx1.get("joe", balanceCol).toString();
    String bal3 = tx1.get("jill", balanceCol).toString();
    String bal4 = tx1.get("jane", balanceCol).toString();
    
    Assert.assertEquals("10", bal1);
    Assert.assertEquals("20", bal2);
    Assert.assertEquals("60", bal3);
    Assert.assertEquals("0", bal4);
    
    tx1.set("bob", balanceCol, (Long.parseLong(bal1) - 5) + "");
    tx1.set("joe", balanceCol, (Long.parseLong(bal2) + 5) + "");
    
    Assert.assertFalse(tx1.commit());
    
    Transaction tx3 = new Transaction("bank2", conn);
    
    Transaction tx4 = new Transaction("bank2", conn);
    tx4.set("jane", balanceCol, "3");
    tx4.commit();
    
    Assert.assertEquals("5", tx3.get("bob", balanceCol).toString());
    Assert.assertEquals("15", tx3.get("joe", balanceCol).toString());
    Assert.assertEquals("70", tx3.get("jill", balanceCol).toString());
    Assert.assertNull(tx3.get("jane", balanceCol));
    
    Transaction tx5 = new Transaction("bank2", conn);
    
    Assert.assertEquals("5", tx5.get("bob", balanceCol).toString());
    Assert.assertEquals("15", tx5.get("joe", balanceCol).toString());
    Assert.assertEquals("70", tx5.get("jill", balanceCol).toString());
    Assert.assertEquals("3", tx5.get("jane", balanceCol).toString());
  }
  
  @Test
  public void testAck() throws Exception {
    // when two transactions run against the same observed column, only one should commit
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    Operations.createTable("ackTest", conn);
    
    Transaction tx = new Transaction("ackTest", conn);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    
    Assert.assertTrue(tx.commit());
    
    Transaction tx1 = new Transaction("ackTest", conn, new ArrayByteSequence("joe"), balanceCol);
    tx1.get("joe", balanceCol);
    tx1.set("jill", balanceCol, "61");
    
    Transaction tx2 = new Transaction("ackTest", conn, new ArrayByteSequence("joe"), balanceCol);
    tx2.get("joe", balanceCol);
    tx2.set("bob", balanceCol, "11");
    
    Assert.assertTrue(tx1.commit());
    Assert.assertFalse(tx2.commit());
    
    Transaction tx3 = new Transaction("ackTest", conn);
    
    Assert.assertEquals("10", tx3.get("bob", balanceCol).toString());
    Assert.assertEquals("20", tx3.get("joe", balanceCol).toString());
    Assert.assertEquals("61", tx3.get("jill", balanceCol).toString());
    
    // update joe, so it can be acknowledged again
    tx3.set("joe", balanceCol, "21");
    
    Assert.assertTrue(tx3.commit());
    
    Transaction tx4 = new Transaction("ackTest", conn, new ArrayByteSequence("joe"), balanceCol);
    tx4.get("joe", balanceCol);
    tx4.set("jill", balanceCol, "62");
    
    Transaction tx5 = new Transaction("ackTest", conn, new ArrayByteSequence("joe"), balanceCol);
    tx5.get("joe", balanceCol);
    tx5.set("bob", balanceCol, "11");
    
    // make the 2nd transaction to start commit 1st
    Assert.assertTrue(tx5.commit());
    Assert.assertFalse(tx4.commit());
    
    Transaction tx6 = new Transaction("ackTest", conn);
    
    Assert.assertEquals("11", tx6.get("bob", balanceCol).toString());
    Assert.assertEquals("21", tx6.get("joe", balanceCol).toString());
    Assert.assertEquals("61", tx6.get("jill", balanceCol).toString());
    
  }
  
  @Test
  public void testWriteObserved() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged status
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    Operations.createTable("ackTest2", conn);
    
    Transaction tx = new Transaction("ackTest2", conn);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    
    Assert.assertTrue(tx.commit());
    
    Transaction tx2 = new Transaction("ackTest2", conn, new ArrayByteSequence("joe"), balanceCol);
    tx2.get("joe", balanceCol);
    tx2.set("joe", balanceCol, "21");
    tx2.set("bob", balanceCol, "11");
    
    Transaction tx1 = new Transaction("ackTest2", conn, new ArrayByteSequence("joe"), balanceCol);
    tx1.get("joe", balanceCol);
    tx1.set("jill", balanceCol, "61");
    
    Assert.assertTrue(tx1.commit());
    Assert.assertFalse(tx2.commit());
    
    Transaction tx3 = new Transaction("ackTest2", conn);
    
    Assert.assertEquals("10", tx3.get("bob", balanceCol).toString());
    Assert.assertEquals("20", tx3.get("joe", balanceCol).toString());
    Assert.assertEquals("61", tx3.get("jill", balanceCol).toString());
    
  }
  
  @Test
  public void testVisibility() {
    // TODO test col vis
  }

  @Test
  public void testRange() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged status
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken("superSecret"));
    
    Operations.createTable("rangeTest", conn);
    
    Transaction tx = new Transaction("rangeTest", conn);
    tx.set("d00001", new Column("data", "content"), "blah blah, blah http://a.com. Blah blah http://b.com.  Blah http://c.com");
    tx.set("d00001", new Column("outlink", "http://a.com"), "");
    tx.set("d00001", new Column("outlink", "http://b.com"), "");
    tx.set("d00001", new Column("outlink", "http://c.com"), "");
    
    tx.set("d00002", new Column("data", "content"), "blah blah, blah http://d.com. Blah blah http://e.com.  Blah http://c.com");
    tx.set("d00002", new Column("outlink", "http://d.com"), "");
    tx.set("d00002", new Column("outlink", "http://e.com"), "");
    tx.set("d00002", new Column("outlink", "http://c.com"), "");
    
    Assert.assertTrue(tx.commit());
    
    Transaction tx2 = new Transaction("rangeTest", conn);
    
    Transaction tx3 = new Transaction("rangeTest", conn);
    
    tx3.set("d00001", new Column("data", "content"), "blah blah, blah http://a.com. Blah http://c.com .  Blah http://z.com");
    tx3.set("d00001", new Column("outlink", "http://a.com"), "");
    tx3.delete("d00001", new Column("outlink", "http://b.com"));
    tx3.set("d00001", new Column("outlink", "http://c.com"), "");
    tx3.set("d00001", new Column("outlink", "http://z.com"), "");
    
    tx3.commit();
    
    HashSet<Column> columns = new HashSet<Column>();
    RowIterator riter = tx2.get(new ScannerConfiguration().setRange(Range.exact("d00001", "outlink")));
    while (riter.hasNext()) {
      ColumnIterator citer = riter.next().getValue();
      while (citer.hasNext()) {
        columns.add(citer.next().getKey());
      }
    }
    
    HashSet<Column> expected = new HashSet<Column>();
    expected.add(new Column("outlink", "http://a.com"));
    expected.add(new Column("outlink", "http://b.com"));
    expected.add(new Column("outlink", "http://c.com"));
    
    Assert.assertEquals(expected, columns);
    
    Transaction tx4 = new Transaction("rangeTest", conn);
    columns.clear();
    riter = tx4.get(new ScannerConfiguration().setRange(Range.exact("d00001", "outlink")));
    while (riter.hasNext()) {
      ColumnIterator citer = riter.next().getValue();
      while (citer.hasNext()) {
        columns.add(citer.next().getKey());
      }
    }
    expected.add(new Column("outlink", "http://z.com"));
    expected.remove(new Column("outlink", "http://b.com"));
    Assert.assertEquals(expected, columns);
    
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    // folder.delete();
  }
}
