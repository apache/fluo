package org.apache.accumulo.accismus;

import java.util.HashSet;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Test;

public class AccismusTest extends TestBase {
  
  @Test
  public void testOverlap1() throws Exception {
    // test transactions that overlap reads and both attempt to write
    // TX1 starts
    // TX2 starts
    // TX1 reads/writes
    // TX2 reads/writes
    // TX2 commits -- succeeds
    // TX1 commits -- fails
    
    Transaction tx = new Transaction(config);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    
    Assert.assertTrue(tx.commit());
    
    tx = new Transaction(config);
    
    String bal1 = tx.get("bob", balanceCol).toString();
    String bal2 = tx.get("joe", balanceCol).toString();
    Assert.assertEquals("10", bal1);
    Assert.assertEquals("20", bal2);
    
    tx.set("bob", balanceCol, (Long.parseLong(bal1) - 5) + "");
    tx.set("joe", balanceCol, (Long.parseLong(bal2) + 5) + "");
    
    Transaction tx2 = new Transaction(config);
    
    String bal3 = tx2.get("bob", balanceCol).toString();
    String bal4 = tx2.get("jill", balanceCol).toString();
    Assert.assertEquals("10", bal3);
    Assert.assertEquals("60", bal4);
    
    tx2.set("bob", balanceCol, (Long.parseLong(bal3) - 5) + "");
    tx2.set("jill", balanceCol, (Long.parseLong(bal4) + 5) + "");
    
    Assert.assertTrue(tx2.commit());
    Assert.assertFalse(tx.commit());
    
    Transaction tx3 = new Transaction(config);
    
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
    
    Transaction tx = new Transaction(config);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    tx.set("jane", balanceCol, "0");
    
    Assert.assertTrue(tx.commit());
    
    Transaction tx1 = new Transaction(config);
    
    Transaction tx2 = new Transaction(config);
    tx2.set("bob", balanceCol, (Long.parseLong(tx2.get("bob", balanceCol).toString()) - 5) + "");
    tx2.set("joe", balanceCol, (Long.parseLong(tx2.get("joe", balanceCol).toString()) - 5) + "");
    tx2.set("jill", balanceCol, (Long.parseLong(tx2.get("jill", balanceCol).toString()) + 10) + "");
    
    String bal1 = tx1.get("bob", balanceCol).toString();
    
    Assert.assertTrue(tx2.commit());
    
    Transaction txd = new Transaction(config);
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
    
    Transaction tx3 = new Transaction(config);
    
    Transaction tx4 = new Transaction(config);
    tx4.set("jane", balanceCol, "3");
    tx4.commit();
    
    Assert.assertEquals("5", tx3.get("bob", balanceCol).toString());
    Assert.assertEquals("15", tx3.get("joe", balanceCol).toString());
    Assert.assertEquals("70", tx3.get("jill", balanceCol).toString());
    Assert.assertNull(tx3.get("jane", balanceCol));
    
    Transaction tx5 = new Transaction(config);
    
    Assert.assertEquals("5", tx5.get("bob", balanceCol).toString());
    Assert.assertEquals("15", tx5.get("joe", balanceCol).toString());
    Assert.assertEquals("70", tx5.get("jill", balanceCol).toString());
    Assert.assertEquals("3", tx5.get("jane", balanceCol).toString());
  }
  
  @Test
  public void testAck() throws Exception {
    // when two transactions run against the same observed column, only one should commit
    
    Transaction tx = new Transaction(config);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    
    Assert.assertTrue(tx.commit());
    
    Transaction tx1 = new Transaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx1.get("joe", balanceCol);
    tx1.set("jill", balanceCol, "61");
    
    Transaction tx2 = new Transaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx2.get("joe", balanceCol);
    tx2.set("bob", balanceCol, "11");
    
    Assert.assertTrue(tx1.commit());
    Assert.assertFalse(tx2.commit());
    
    Transaction tx3 = new Transaction(config);
    
    Assert.assertEquals("10", tx3.get("bob", balanceCol).toString());
    Assert.assertEquals("20", tx3.get("joe", balanceCol).toString());
    Assert.assertEquals("61", tx3.get("jill", balanceCol).toString());
    
    // update joe, so it can be acknowledged again
    tx3.set("joe", balanceCol, "21");
    
    Assert.assertTrue(tx3.commit());
    
    Transaction tx4 = new Transaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx4.get("joe", balanceCol);
    tx4.set("jill", balanceCol, "62");
    
    Transaction tx5 = new Transaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx5.get("joe", balanceCol);
    tx5.set("bob", balanceCol, "11");
    
    // make the 2nd transaction to start commit 1st
    Assert.assertTrue(tx5.commit());
    Assert.assertFalse(tx4.commit());
    
    Transaction tx6 = new Transaction(config);
    
    Assert.assertEquals("11", tx6.get("bob", balanceCol).toString());
    Assert.assertEquals("21", tx6.get("joe", balanceCol).toString());
    Assert.assertEquals("61", tx6.get("jill", balanceCol).toString());
    
  }
  
  @Test
  public void testWriteObserved() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged status
    
    Transaction tx = new Transaction(config);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    
    Assert.assertTrue(tx.commit());
    
    Transaction tx2 = new Transaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx2.get("joe", balanceCol);
    tx2.set("joe", balanceCol, "21");
    tx2.set("bob", balanceCol, "11");
    
    Transaction tx1 = new Transaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx1.get("joe", balanceCol);
    tx1.set("jill", balanceCol, "61");
    
    Assert.assertTrue(tx1.commit());
    Assert.assertFalse(tx2.commit());
    
    Transaction tx3 = new Transaction(config);
    
    Assert.assertEquals("10", tx3.get("bob", balanceCol).toString());
    Assert.assertEquals("20", tx3.get("joe", balanceCol).toString());
    Assert.assertEquals("61", tx3.get("jill", balanceCol).toString());
    
  }
  
  @Test
  public void testVisibility() throws Exception {

    conn.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C"));

    config.setAuthorizations(new Authorizations("A", "B", "C"));
    
    Column balanceCol = new Column("account", "balance");
    balanceCol.setVisibility(new ColumnVisibility("A|B"));

    Transaction tx = new Transaction(config);
    
    tx.set("bob", balanceCol, "10");
    tx.set("joe", balanceCol, "20");
    tx.set("jill", balanceCol, "60");
    
    Assert.assertTrue(tx.commit());
    
    Configuration config2 = new Configuration(zk, zkn, conn);
    config2.setAuthorizations(new Authorizations("B"));
    
    Transaction tx2 = new Transaction(config2);
    Assert.assertEquals("10", tx2.get("bob", balanceCol).toString());
    Assert.assertEquals("20", tx2.get("joe", balanceCol).toString());
    Assert.assertEquals("60", tx2.get("jill", balanceCol).toString());
    
    Configuration config3 = new Configuration(zk, zkn, conn);
    config3.setAuthorizations(new Authorizations("C"));
    
    Transaction tx3 = new Transaction(config3);
    Assert.assertNull(tx3.get("bob", balanceCol));
    Assert.assertNull(tx3.get("joe", balanceCol));
    Assert.assertNull(tx3.get("jill", balanceCol));
  }

  @Test
  public void testRange() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged status
    
    Transaction tx = new Transaction(config);
    tx.set("d00001", new Column("data", "content"), "blah blah, blah http://a.com. Blah blah http://b.com.  Blah http://c.com");
    tx.set("d00001", new Column("outlink", "http://a.com"), "");
    tx.set("d00001", new Column("outlink", "http://b.com"), "");
    tx.set("d00001", new Column("outlink", "http://c.com"), "");
    
    tx.set("d00002", new Column("data", "content"), "blah blah, blah http://d.com. Blah blah http://e.com.  Blah http://c.com");
    tx.set("d00002", new Column("outlink", "http://d.com"), "");
    tx.set("d00002", new Column("outlink", "http://e.com"), "");
    tx.set("d00002", new Column("outlink", "http://c.com"), "");
    
    Assert.assertTrue(tx.commit());
    
    Transaction tx2 = new Transaction(config);
    
    Transaction tx3 = new Transaction(config);
    
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
    
    Transaction tx4 = new Transaction(config);
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
}
