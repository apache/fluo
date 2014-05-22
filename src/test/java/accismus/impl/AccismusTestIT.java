package accismus.impl;

import java.util.HashSet;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.junit.Assert;
import org.junit.Test;

import accismus.api.Column;
import accismus.api.ColumnIterator;
import accismus.api.RowIterator;
import accismus.api.ScannerConfiguration;
import accismus.api.exceptions.AlreadyAcknowledgedException;
import accismus.api.exceptions.CommitException;

public class AccismusTestIT extends Base {
  
  @Test
  public void testOverlap1() throws Exception {
    // test transactions that overlap reads and both attempt to write
    // TX1 starts
    // TX2 starts
    // TX1 reads/writes
    // TX2 reads/writes
    // TX2 commits -- succeeds
    // TX1 commits -- fails
    
    TestTransaction tx = new TestTransaction(config);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.sete("bob", balanceCol).from("10");
    tx.sete("joe", balanceCol).from("20");
    tx.sete("jill", balanceCol).from("60");
    
    tx.commit();
    
    tx = new TestTransaction(config);
    
    String bal1 = tx.getd("bob", balanceCol).toString();
    String bal2 = tx.getd("joe", balanceCol).toString();
    Assert.assertEquals("10", bal1);
    Assert.assertEquals("20", bal2);
    
    tx.sete("bob", balanceCol).from((Long.parseLong(bal1) - 5) + "");
    tx.sete("joe", balanceCol).from((Long.parseLong(bal2) + 5) + "");
    
    TestTransaction tx2 = new TestTransaction(config);
    
    String bal3 = tx2.getd("bob", balanceCol).toString();
    String bal4 = tx2.getd("jill", balanceCol).toString();
    Assert.assertEquals("10", bal3);
    Assert.assertEquals("60", bal4);
    
    tx2.sete("bob", balanceCol).from((Long.parseLong(bal3) - 5) + "");
    tx2.sete("jill", balanceCol).from((Long.parseLong(bal4) + 5) + "");
    
    tx2.commit();
    assertCommitFails(tx);
    
    TestTransaction tx3 = new TestTransaction(config);
    
    Assert.assertEquals("5", tx3.getd("bob", balanceCol).toString());
    Assert.assertEquals("20", tx3.getd("joe", balanceCol).toString());
    Assert.assertEquals("65", tx3.getd("jill", balanceCol).toString());
  }
  
  private void assertCommitFails(TestTransaction tx) {
    try {
      tx.commit();
      Assert.fail();
    } catch (CommitException ce) {
      
    }
  }
  
  @Test
  public void testSnapshots() throws Exception {
    // test the following case
    // TX1 starts
    // TX2 starts
    // TX2 reads/writes
    // TX2 commits
    // TX1 reads -- should not see TX2 writes
    
    TestTransaction tx = new TestTransaction(config);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.sete("bob", balanceCol).from(10);
    tx.sete("joe", balanceCol).from(20);
    tx.sete("jill", balanceCol).from(60);
    tx.sete("jane", balanceCol).from(0);
    
    tx.commit();
    
    TestTransaction tx1 = new TestTransaction(config);
    
    TestTransaction tx2 = new TestTransaction(config);
    
    tx2.sete("bob", balanceCol).from(tx2.getd("bob", balanceCol).toLong() - 5);
    tx2.sete("joe", balanceCol).from(tx2.getd("joe", balanceCol).toLong() - 5);
    tx2.sete("jill", balanceCol).from(tx2.getd("jill", balanceCol).toLong() + 10);

    long bal1 = tx1.getd("bob", balanceCol).toLong();
    
    tx2.commit();
    
    TestTransaction txd = new TestTransaction(config);
    txd.delete("jane", balanceCol);
    txd.commit();
    
    long bal2 = tx1.getd("joe", balanceCol).toLong();
    long bal3 = tx1.getd("jill", balanceCol).toLong();
    long bal4 = tx1.getd("jane", balanceCol).toLong();
    
    Assert.assertEquals(10l, bal1);
    Assert.assertEquals(20l, bal2);
    Assert.assertEquals(60l, bal3);
    Assert.assertEquals(0l, bal4);
    
    tx1.sete("bob", balanceCol).from(bal1 - 5);
    tx1.sete("joe", balanceCol).from(bal2 + 5);
    
    assertCommitFails(tx1);
    
    TestTransaction tx3 = new TestTransaction(config);
    
    TestTransaction tx4 = new TestTransaction(config);
    tx4.sete("jane", balanceCol).from(3);
    tx4.commit();
    
    Assert.assertEquals(5l, tx3.getd("bob", balanceCol).toLong(0));
    Assert.assertEquals(15l, tx3.getd("joe", balanceCol).toLong(0));
    Assert.assertEquals(70l, tx3.getd("jill", balanceCol).toLong(0));
    Assert.assertNull(tx3.getd("jane", balanceCol).toLong());
    
    TestTransaction tx5 = new TestTransaction(config);
    
    Assert.assertEquals(5l, tx5.getd("bob", balanceCol).toLong(0));
    Assert.assertEquals(15l, tx5.getd("joe", balanceCol).toLong(0));
    Assert.assertEquals(70l, tx5.getd("jill", balanceCol).toLong(0));
    Assert.assertEquals(3l, tx5.getd("jane", balanceCol).toLong(0));
  }
  
  @Test
  public void testAck() throws Exception {
    // when two transactions run against the same observed column, only one should commit
    
    TestTransaction tx = new TestTransaction(config);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.sete("bob", balanceCol).from("10");
    tx.sete("joe", balanceCol).from("20");
    tx.sete("jill", balanceCol).from("60");
    
    tx.commit();
    
    TestTransaction tx1 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx1.getd("joe", balanceCol);
    tx1.sete("jill", balanceCol).from("61");
    
    TestTransaction tx2 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx2.getd("joe", balanceCol);
    tx2.sete("bob", balanceCol).from("11");
    
    tx1.commit();
    assertCommitFails(tx2);
    
    TestTransaction tx3 = new TestTransaction(config);
    
    Assert.assertEquals("10", tx3.getd("bob", balanceCol).toString());
    Assert.assertEquals("20", tx3.getd("joe", balanceCol).toString());
    Assert.assertEquals("61", tx3.getd("jill", balanceCol).toString());
    
    // update joe, so it can be acknowledged again
    tx3.sete("joe", balanceCol).from("21");
    
    tx3.commit();
    
    TestTransaction tx4 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx4.getd("joe", balanceCol);
    tx4.sete("jill", balanceCol).from("62");
    
    TestTransaction tx5 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx5.getd("joe", balanceCol);
    tx5.sete("bob", balanceCol).from("11");
    
    // make the 2nd transaction to start commit 1st
    tx5.commit();
    assertCommitFails(tx4);
    
    TestTransaction tx6 = new TestTransaction(config);
    
    Assert.assertEquals("11", tx6.getd("bob", balanceCol).toString());
    Assert.assertEquals("21", tx6.getd("joe", balanceCol).toString());
    Assert.assertEquals("61", tx6.getd("jill", balanceCol).toString());
    
    TestTransaction tx7 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx7.getd("joe", balanceCol);
    tx7.sete("bob", balanceCol).from("15");
    tx7.sete("jill", balanceCol).from("60");
    
    try {
      tx7.commit();
      Assert.fail();
    } catch (AlreadyAcknowledgedException aae) {}
    
    TestTransaction tx8 = new TestTransaction(config);
    
    Assert.assertEquals("11", tx8.getd("bob", balanceCol).toString());
    Assert.assertEquals("21", tx8.getd("joe", balanceCol).toString());
    Assert.assertEquals("61", tx8.getd("jill", balanceCol).toString());
  }
  
  @Test
  public void testWriteObserved() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged status
    
    TestTransaction tx = new TestTransaction(config);
    
    Column balanceCol = new Column("account", "balance");
    
    tx.sete("bob", balanceCol).from("10");
    tx.sete("joe", balanceCol).from("20");
    tx.sete("jill", balanceCol).from("60");
    
    tx.commit();
    
    TestTransaction tx2 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx2.getd("joe", balanceCol);
    tx2.sete("joe", balanceCol).from("21");
    tx2.sete("bob", balanceCol).from("11");
    
    TestTransaction tx1 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx1.getd("joe", balanceCol);
    tx1.sete("jill", balanceCol).from("61");
    
    tx1.commit();
    assertCommitFails(tx2);
    
    TestTransaction tx3 = new TestTransaction(config);
    
    Assert.assertEquals("10", tx3.getd("bob", balanceCol).toString());
    Assert.assertEquals("20", tx3.getd("joe", balanceCol).toString());
    Assert.assertEquals("61", tx3.getd("jill", balanceCol).toString());
    
  }
  
  @Test
  public void testVisibility() throws Exception {

    conn.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C"));

    config.setAuthorizations(new Authorizations("A", "B", "C"));
    
    Column balanceCol = new Column("account", "balance");
    balanceCol.setVisibility(new ColumnVisibility("A|B"));

    TestTransaction tx = new TestTransaction(config);
    
    tx.sete("bob", balanceCol).from("10");
    tx.sete("joe", balanceCol).from("20");
    tx.sete("jill", balanceCol).from("60");
    
    tx.commit();
    
    Configuration config2 = new Configuration(zk, zkn, conn);
    config2.setAuthorizations(new Authorizations("B"));
    
    TestTransaction tx2 = new TestTransaction(config2);
    Assert.assertEquals("10", tx2.getd("bob", balanceCol).toString());
    Assert.assertEquals("20", tx2.getd("joe", balanceCol).toString());
    Assert.assertEquals("60", tx2.getd("jill", balanceCol).toString());
    
    Configuration config3 = new Configuration(zk, zkn, conn);
    config3.setAuthorizations(new Authorizations("C"));
    
    TestTransaction tx3 = new TestTransaction(config3);
    Assert.assertNull(tx3.getd("bob", balanceCol).toString());
    Assert.assertNull(tx3.getd("joe", balanceCol).toString());
    Assert.assertNull(tx3.getd("jill", balanceCol).toString());
  }

  @Test
  public void testRange() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged status
    
    TestTransaction tx = new TestTransaction(config);
    tx.sete("d00001", new Column("data", "content")).from("blah blah, blah http://a.com. Blah blah http://b.com.  Blah http://c.com");
    tx.sete("d00001", new Column("outlink", "http://a.com")).from("");
    tx.sete("d00001", new Column("outlink", "http://b.com")).from("");
    tx.sete("d00001", new Column("outlink", "http://c.com")).from("");
    
    tx.sete("d00002", new Column("data", "content")).from("blah blah, blah http://d.com. Blah blah http://e.com.  Blah http://c.com");
    tx.sete("d00002", new Column("outlink", "http://d.com")).from("");
    tx.sete("d00002", new Column("outlink", "http://e.com")).from("");
    tx.sete("d00002", new Column("outlink", "http://c.com")).from("");
    
    tx.commit();
    
    TestTransaction tx2 = new TestTransaction(config);
    
    TestTransaction tx3 = new TestTransaction(config);
    
    tx3.sete("d00001", new Column("data", "content")).from("blah blah, blah http://a.com. Blah http://c.com .  Blah http://z.com");
    tx3.sete("d00001", new Column("outlink", "http://a.com")).from("");
    tx3.delete("d00001", new Column("outlink", "http://b.com"));
    tx3.sete("d00001", new Column("outlink", "http://c.com")).from("");
    tx3.sete("d00001", new Column("outlink", "http://z.com")).from("");
    
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
    
    TestTransaction tx4 = new TestTransaction(config);
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
