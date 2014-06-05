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
import accismus.api.types.StringEncoder;
import accismus.api.types.TypeLayer;
import accismus.impl.TransactionImpl.CommitData;

public class AccismusIT extends Base {
  
  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

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
    
    Column balanceCol = typeLayer.newColumn("account", "balance");
    
    tx.set().row("bob").col(balanceCol).val("10");
    tx.set().row("joe").col(balanceCol).val("20");
    tx.set().row("jill").col(balanceCol).val("60");
    
    tx.commit();
    
    tx = new TestTransaction(config);
    
    String bal1 = tx.get().row("bob").col(balanceCol).toString();
    String bal2 = tx.get().row("joe").col(balanceCol).toString();
    Assert.assertEquals("10", bal1);
    Assert.assertEquals("20", bal2);
    
    tx.set().row("bob").col(balanceCol).val((Long.parseLong(bal1) - 5) + "");
    tx.set().row("joe").col(balanceCol).val((Long.parseLong(bal2) + 5) + "");
    
    TestTransaction tx2 = new TestTransaction(config);
    
    String bal3 = tx2.get().row("bob").col(balanceCol).toString();
    String bal4 = tx2.get().row("jill").col(balanceCol).toString();
    Assert.assertEquals("10", bal3);
    Assert.assertEquals("60", bal4);
    
    tx2.set().row("bob").col(balanceCol).val((Long.parseLong(bal3) - 5) + "");
    tx2.set().row("jill").col(balanceCol).val((Long.parseLong(bal4) + 5) + "");
    
    tx2.commit();
    assertCommitFails(tx);
    
    TestTransaction tx3 = new TestTransaction(config);
    
    Assert.assertEquals("5", tx3.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("20", tx3.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("65", tx3.get().row("jill").col(balanceCol).toString());
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
    
    Column balanceCol = typeLayer.newColumn("account", "balance");
    
    tx.set().row("bob").col(balanceCol).val(10);
    tx.set().row("joe").col(balanceCol).val(20);
    tx.set().row("jill").col(balanceCol).val(60);
    tx.set().row("jane").col(balanceCol).val(0);
    
    tx.commit();
    
    TestTransaction tx1 = new TestTransaction(config);
    
    TestTransaction tx2 = new TestTransaction(config);
    
    tx2.set().row("bob").col(balanceCol).val(tx2.get().row("bob").col(balanceCol).toLong() - 5);
    tx2.set().row("joe").col(balanceCol).val(tx2.get().row("joe").col(balanceCol).toLong() - 5);
    tx2.set().row("jill").col(balanceCol).val(tx2.get().row("jill").col(balanceCol).toLong() + 10);

    long bal1 = tx1.get().row("bob").col(balanceCol).toLong();
    
    tx2.commit();
    
    TestTransaction txd = new TestTransaction(config);
    txd.delete().row("jane").col(balanceCol);
    txd.commit();
    
    long bal2 = tx1.get().row("joe").col(balanceCol).toLong();
    long bal3 = tx1.get().row("jill").col(balanceCol).toLong();
    long bal4 = tx1.get().row("jane").col(balanceCol).toLong();
    
    Assert.assertEquals(10l, bal1);
    Assert.assertEquals(20l, bal2);
    Assert.assertEquals(60l, bal3);
    Assert.assertEquals(0l, bal4);
    
    tx1.set().row("bob").col(balanceCol).val(bal1 - 5);
    tx1.set().row("joe").col(balanceCol).val(bal2 + 5);
    
    assertCommitFails(tx1);
    
    TestTransaction tx3 = new TestTransaction(config);
    
    TestTransaction tx4 = new TestTransaction(config);
    tx4.set().row("jane").col(balanceCol).val(3);
    tx4.commit();
    
    Assert.assertEquals(5l, tx3.get().row("bob").col(balanceCol).toLong(0));
    Assert.assertEquals(15l, tx3.get().row("joe").col(balanceCol).toLong(0));
    Assert.assertEquals(70l, tx3.get().row("jill").col(balanceCol).toLong(0));
    Assert.assertNull(tx3.get().row("jane").col(balanceCol).toLong());
    
    TestTransaction tx5 = new TestTransaction(config);
    
    Assert.assertEquals(5l, tx5.get().row("bob").col(balanceCol).toLong(0));
    Assert.assertEquals(15l, tx5.get().row("joe").col(balanceCol).toLong(0));
    Assert.assertEquals(70l, tx5.get().row("jill").col(balanceCol).toLong(0));
    Assert.assertEquals(3l, tx5.get().row("jane").col(balanceCol).toLong(0));
  }
  
  @Test
  public void testAck() throws Exception {
    // when two transactions run against the same observed column, only one should commit
    
    TestTransaction tx = new TestTransaction(config);
    
    Column balanceCol = typeLayer.newColumn("account", "balance");
    
    tx.set().row("bob").col(balanceCol).val("10");
    tx.set().row("joe").col(balanceCol).val("20");
    tx.set().row("jill").col(balanceCol).val("60");
    
    tx.commit();
    
    TestTransaction tx1 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx1.get().row("joe").col(balanceCol);
    tx1.set().row("jill").col(balanceCol).val("61");
    
    TestTransaction tx2 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx2.get().row("joe").col(balanceCol);
    tx2.set().row("bob").col(balanceCol).val("11");
    
    tx1.commit();
    assertCommitFails(tx2);
    
    TestTransaction tx3 = new TestTransaction(config);
    
    Assert.assertEquals("10", tx3.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("20", tx3.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("61", tx3.get().row("jill").col(balanceCol).toString());
    
    // update joe, so it can be acknowledged again
    tx3.set().row("joe").col(balanceCol).val("21");
    
    tx3.commit();
    
    TestTransaction tx4 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx4.get().row("joe").col(balanceCol);
    tx4.set().row("jill").col(balanceCol).val("62");
    
    TestTransaction tx5 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx5.get().row("joe").col(balanceCol);
    tx5.set().row("bob").col(balanceCol).val("11");
    
    // make the 2nd transaction to start commit 1st
    tx5.commit();
    assertCommitFails(tx4);
    
    TestTransaction tx6 = new TestTransaction(config);
    
    Assert.assertEquals("11", tx6.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("21", tx6.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("61", tx6.get().row("jill").col(balanceCol).toString());
    
    TestTransaction tx7 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx7.get().row("joe").col(balanceCol);
    tx7.set().row("bob").col(balanceCol).val("15");
    tx7.set().row("jill").col(balanceCol).val("60");
    
    try {
      tx7.commit();
      Assert.fail();
    } catch (AlreadyAcknowledgedException aae) {}
    
    TestTransaction tx8 = new TestTransaction(config);
    
    Assert.assertEquals("11", tx8.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("21", tx8.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("61", tx8.get().row("jill").col(balanceCol).toString());
  }
  
  @Test
  public void testAck2() throws Exception {
    TestTransaction tx = new TestTransaction(config);

    Column balanceCol = typeLayer.newColumn("account", "balance");
    Column addrCol = typeLayer.newColumn("account", "addr");

    tx.set().row("bob").col(balanceCol).val("10");
    tx.set().row("joe").col(balanceCol).val("20");
    tx.set().row("jill").col(balanceCol).val("60");

    tx.commit();

    TestTransaction tx1 = new TestTransaction(config, "bob", balanceCol);
    TestTransaction tx2 = new TestTransaction(config, "bob", balanceCol);

    tx1.get().row("bob").col(balanceCol).toString();
    tx2.get().row("bob").col(balanceCol).toString();

    tx1.get().row("bob").col(addrCol).toString();
    tx2.get().row("bob").col(addrCol).toString();

    tx1.set().row("bob").col(addrCol).val("1 loop pl");
    tx2.set().row("bob").col(addrCol).val("1 loop pl");

    // this test overlaps the commmits of two transactions w/ the same trigger

    CommitData cd = tx1.createCommitData();
    Assert.assertTrue(tx1.preCommit(cd));

    try {
      tx2.commit();
    } catch (CommitException ce) {

    }

    long commitTs = OracleClient.getInstance(config).getTimestamp();
    Assert.assertTrue(tx1.commitPrimaryColumn(cd, commitTs));
    tx1.finishCommit(cd, commitTs);

    TestTransaction tx3 = new TestTransaction(config, "bob", balanceCol);
    tx3.set().row("bob").col(addrCol).val("2 loop pl");
    try {
      tx3.commit();
    } catch (AlreadyAcknowledgedException e) {

    }

  }

  @Test
  public void testWriteObserved() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged status
    
    TestTransaction tx = new TestTransaction(config);
    
    Column balanceCol = typeLayer.newColumn("account", "balance");
    
    tx.set().row("bob").col(balanceCol).val("10");
    tx.set().row("joe").col(balanceCol).val("20");
    tx.set().row("jill").col(balanceCol).val("60");
    
    tx.commit();
    
    TestTransaction tx2 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx2.get().row("joe").col(balanceCol);
    tx2.set().row("joe").col(balanceCol).val("21");
    tx2.set().row("bob").col(balanceCol).val("11");
    
    TestTransaction tx1 = new TestTransaction(config, new ArrayByteSequence("joe"), balanceCol);
    tx1.get().row("joe").col(balanceCol);
    tx1.set().row("jill").col(balanceCol).val("61");
    
    tx1.commit();
    assertCommitFails(tx2);
    
    TestTransaction tx3 = new TestTransaction(config);
    
    Assert.assertEquals("10", tx3.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("20", tx3.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("61", tx3.get().row("jill").col(balanceCol).toString());
    
  }
  
  @Test
  public void testVisibility() throws Exception {

    conn.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B", "C"));

    config.setAuthorizations(new Authorizations("A", "B", "C"));
    
    Column balanceCol = typeLayer.newColumn("account", "balance");
    balanceCol.setVisibility(new ColumnVisibility("A|B"));

    TestTransaction tx = new TestTransaction(config);
    
    tx.set().row("bob").col(balanceCol).val("10");
    tx.set().row("joe").col(balanceCol).val("20");
    tx.set().row("jill").col(balanceCol).val("60");
    
    tx.commit();
    
    Configuration config2 = new Configuration(zk, zkn, conn);
    config2.setAuthorizations(new Authorizations("B"));
    
    TestTransaction tx2 = new TestTransaction(config2);
    Assert.assertEquals("10", tx2.get().row("bob").col(balanceCol).toString());
    Assert.assertEquals("20", tx2.get().row("joe").col(balanceCol).toString());
    Assert.assertEquals("60", tx2.get().row("jill").col(balanceCol).toString());
    
    Configuration config3 = new Configuration(zk, zkn, conn);
    config3.setAuthorizations(new Authorizations("C"));
    
    TestTransaction tx3 = new TestTransaction(config3);
    Assert.assertNull(tx3.get().row("bob").col(balanceCol).toString());
    Assert.assertNull(tx3.get().row("joe").col(balanceCol).toString());
    Assert.assertNull(tx3.get().row("jill").col(balanceCol).toString());
  }

  @Test
  public void testRange() throws Exception {
    // setting an acknowledged observed column in a transaction should not affect acknowledged status
    
    TestTransaction tx = new TestTransaction(config);
    tx.set().row("d00001").col(typeLayer.newColumn("data", "content")).val("blah blah, blah http://a.com. Blah blah http://b.com.  Blah http://c.com");
    tx.set().row("d00001").col(typeLayer.newColumn("outlink", "http://a.com")).val("");
    tx.set().row("d00001").col(typeLayer.newColumn("outlink", "http://b.com")).val("");
    tx.set().row("d00001").col(typeLayer.newColumn("outlink", "http://c.com")).val("");
    
    tx.set().row("d00002").col(typeLayer.newColumn("data", "content")).val("blah blah, blah http://d.com. Blah blah http://e.com.  Blah http://c.com");
    tx.set().row("d00002").col(typeLayer.newColumn("outlink", "http://d.com")).val("");
    tx.set().row("d00002").col(typeLayer.newColumn("outlink", "http://e.com")).val("");
    tx.set().row("d00002").col(typeLayer.newColumn("outlink", "http://c.com")).val("");
    
    tx.commit();
    
    TestTransaction tx2 = new TestTransaction(config);
    
    TestTransaction tx3 = new TestTransaction(config);
    
    tx3.set().row("d00001").col(typeLayer.newColumn("data", "content")).val("blah blah, blah http://a.com. Blah http://c.com .  Blah http://z.com");
    tx3.set().row("d00001").col(typeLayer.newColumn("outlink", "http://a.com")).val("");
    tx3.delete().row("d00001").col(typeLayer.newColumn("outlink", "http://b.com"));
    tx3.set().row("d00001").col(typeLayer.newColumn("outlink", "http://c.com")).val("");
    tx3.set().row("d00001").col(typeLayer.newColumn("outlink", "http://z.com")).val("");
    
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
    expected.add(typeLayer.newColumn("outlink", "http://a.com"));
    expected.add(typeLayer.newColumn("outlink", "http://b.com"));
    expected.add(typeLayer.newColumn("outlink", "http://c.com"));
    
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
    expected.add(typeLayer.newColumn("outlink", "http://z.com"));
    expected.remove(typeLayer.newColumn("outlink", "http://b.com"));
    Assert.assertEquals(expected, columns);
    
  }
}
