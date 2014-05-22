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
package accismus.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Assert;
import org.junit.Test;

import accismus.api.Column;
import accismus.api.exceptions.AlreadyAcknowledgedException;
import accismus.api.exceptions.CommitException;
import accismus.api.exceptions.StaleScanException;
import accismus.impl.TransactionImpl.CommitData;

/**
 * 
 */
public class FailureTestIT extends Base {
  
  
  Column balanceCol = new Column("account", "balance");
    
  private void transfer(Configuration config, String from, String to, int amount) throws Exception {
    TestTransaction tx = new TestTransaction(config);
    
    int bal1 = Integer.parseInt(tx.getd(from, balanceCol).toString());
    int bal2 = Integer.parseInt(tx.getd(to, balanceCol).toString());
    
    tx.sete(from, balanceCol).from("" + (bal1 - amount));
    tx.sete(to, balanceCol).from("" + (bal2 + amount));
    
    tx.commit();
  }

  protected Map<Column,String> getObservers() {
    Map<Column,String> observed = new HashMap<Column,String>();
    observed.put(new Column("attr", "lastupdate"), "foo");
    return observed;
  }

  @Test
  public void testRollbackMany() throws Exception {
    // test writing lots of columns that need to be rolled back

    Column col1 = new Column("fam1", "q1");
    Column col2 = new Column("fam1", "q2");
    
    TestTransaction tx = new TestTransaction(config);
    
    for (int r = 0; r < 10; r++) {
      tx.sete(r + "", col1).from("0" + r + "0");
      tx.sete(r + "", col2).from("0" + r + "1");
    }
    
    tx.commit();
    
    TestTransaction tx2 = new TestTransaction(config);
    
    for (int r = 0; r < 10; r++) {
      tx2.sete(r + "", col1).from("1" + r + "0");
      tx2.sete(r + "", col2).from("1" + r + "1");
    }
    
    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));
    
    TestTransaction tx3 = new TestTransaction(config);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("0" + r + "0", tx3.getd(r + "", col1).toString());
      Assert.assertEquals("0" + r + "1", tx3.getd(r + "", col2).toString());
    }
    
    long commitTs = OracleClient.getInstance(config).getTimestamp();
    Assert.assertFalse(tx2.commitPrimaryColumn(cd, commitTs));

    TestTransaction tx4 = new TestTransaction(config);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("0" + r + "0", tx4.getd(r + "", col1).toString());
      Assert.assertEquals("0" + r + "1", tx4.getd(r + "", col2).toString());
    }

  }

  @Test
  public void testRollforwardMany() throws Exception {
    // test writing lots of columns that need to be rolled forward
    
    Column col1 = new Column("fam1", "q1");
    Column col2 = new Column("fam1", "q2");
    
    TestTransaction tx = new TestTransaction(config);
    
    for (int r = 0; r < 10; r++) {
      tx.sete(r + "", col1).from("0" + r + "0");
      tx.sete(r + "", col2).from("0" + r + "1");
    }
    
    tx.commit();
    
    TestTransaction tx2 = new TestTransaction(config);
    
    for (int r = 0; r < 10; r++) {
      tx2.sete(r + "", col1).from("1" + r + "0");
      tx2.sete(r + "", col2).from("1" + r + "1");
    }
    
    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));
    long commitTs = OracleClient.getInstance(config).getTimestamp();
    Assert.assertTrue(tx2.commitPrimaryColumn(cd, commitTs));
    
    TestTransaction tx3 = new TestTransaction(config);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("1" + r + "0", tx3.getd(r + "", col1).toString());
      Assert.assertEquals("1" + r + "1", tx3.getd(r + "", col2).toString());
    }
    
    tx2.finishCommit(cd, commitTs);

    TestTransaction tx4 = new TestTransaction(config);
    for (int r = 0; r < 10; r++) {
      Assert.assertEquals("1" + r + "0", tx4.getd(r + "", col1).toString());
      Assert.assertEquals("1" + r + "1", tx4.getd(r + "", col2).toString());
    }
    
  }
  
  @Test
  public void testRollback() throws Exception {
    // test the case where a scan encounters a stuck lock and rolls it back
    
    TestTransaction tx = new TestTransaction(config);
    
    tx.sete("bob", balanceCol).from("10");
    tx.sete("joe", balanceCol).from("20");
    tx.sete("jill", balanceCol).from("60");
    
    tx.commit();
    
    TestTransaction tx2 = new TestTransaction(config);
    
    int bal1 = Integer.parseInt(tx2.getd("bob", balanceCol).toString());
    int bal2 = Integer.parseInt(tx2.getd("joe", balanceCol).toString());
    
    tx2.sete("bob", balanceCol).from("" + (bal1 - 7));
    tx2.sete("joe", balanceCol).from("" + (bal2 + 7));
    
    // get locks
    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));
    
    // test rolling back primary and non-primary columns

    int bobBal = 10;
    int joeBal = 20;
    if ((new Random()).nextBoolean()) {
      transfer(config, "joe", "jill", 7);
      joeBal -= 7;
    } else {
      transfer(config, "bob", "jill", 7);
      bobBal -= 7;
    }
    
    TestTransaction tx4 = new TestTransaction(config);
    
    Assert.assertEquals(bobBal + "", tx4.getd("bob", balanceCol).toString());
    Assert.assertEquals(joeBal + "", tx4.getd("joe", balanceCol).toString());
    Assert.assertEquals("67", tx4.getd("jill", balanceCol).toString());
    
    long commitTs = OracleClient.getInstance(config).getTimestamp();
    Assert.assertFalse(tx2.commitPrimaryColumn(cd, commitTs));
    
    transfer(config, "bob", "joe", 2);
    bobBal -= 2;
    joeBal += 2;
    
    TestTransaction tx6 = new TestTransaction(config);
    
    Assert.assertEquals(bobBal + "", tx6.getd("bob", balanceCol).toString());
    Assert.assertEquals(joeBal + "", tx6.getd("joe", balanceCol).toString());
    Assert.assertEquals("67", tx6.getd("jill", balanceCol).toString());
  }

  @Test
  public void testRollfoward() throws Exception {
    // test the case where a scan encounters a stuck lock (for a complete tx) and rolls it forward
    
    TestTransaction tx = new TestTransaction(config);

    tx.sete("bob", balanceCol).from("10");
    tx.sete("joe", balanceCol).from("20");
    tx.sete("jill", balanceCol).from("60");
    
    tx.commit();
    
    TestTransaction tx2 = new TestTransaction(config);
    
    int bal1 = Integer.parseInt(tx2.getd("bob", balanceCol).toString());
    int bal2 = Integer.parseInt(tx2.getd("joe", balanceCol).toString());
    
    tx2.sete("bob", balanceCol).from("" + (bal1 - 7));
    tx2.sete("joe", balanceCol).from("" + (bal2 + 7));
    
    // get locks
    CommitData cd = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd));
    long commitTs = OracleClient.getInstance(config).getTimestamp();
    Assert.assertTrue(tx2.commitPrimaryColumn(cd, commitTs));

    // test rolling forward primary and non-primary columns
    String bobBal = "3";
    String joeBal = "27";
    if ((new Random()).nextBoolean()) {
      transfer(config, "joe", "jill", 2);
      joeBal = "25";
    } else {
      transfer(config, "bob", "jill", 2);
      bobBal = "1";
    }
    
    TestTransaction tx4 = new TestTransaction(config);
    
    Assert.assertEquals(bobBal, tx4.getd("bob", balanceCol).toString());
    Assert.assertEquals(joeBal, tx4.getd("joe", balanceCol).toString());
    Assert.assertEquals("62", tx4.getd("jill", balanceCol).toString());

    tx2.finishCommit(cd, commitTs);
    
    TestTransaction tx5 = new TestTransaction(config);
    
    Assert.assertEquals(bobBal, tx5.getd("bob", balanceCol).toString());
    Assert.assertEquals(joeBal, tx5.getd("joe", balanceCol).toString());
    Assert.assertEquals("62", tx5.getd("jill", balanceCol).toString());
  }
  
  @Test
  public void testAcks() throws Exception {
    // TODO test that acks are properly handled in rollback and rollforward
    
    TestTransaction tx = new TestTransaction(config);
    
    tx.sete("url0000", new Column("attr", "lastupdate")).from("3");
    tx.sete("url0000", new Column("doc", "content")).from("abc def");
    
    tx.commit();

    TestTransaction tx2 = new TestTransaction(config, new ArrayByteSequence("url0000"), new Column("attr", "lastupdate"));
    tx2.sete("idx:abc", new Column("doc","url")).from("url0000");
    tx2.sete("idx:def", new Column("doc","url")).from("url0000");
    CommitData cd = tx2.createCommitData();
    tx2.preCommit(cd);
    
    TestTransaction tx3 = new TestTransaction(config);
    Assert.assertNull(tx3.getd("idx:abc", new Column("doc", "url")).toString());
    Assert.assertNull(tx3.getd("idx:def", new Column("doc", "url")).toString());
    Assert.assertEquals("3", tx3.getd("url0000", new Column("attr", "lastupdate")).toString());

    Scanner scanner = config.getConnector().createScanner(config.getTable(), Authorizations.EMPTY);
    scanner.fetchColumnFamily(ByteUtil.toText(Constants.NOTIFY_CF));
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals("url0000", iter.next().getKey().getRow().toString());
    
    TestTransaction tx5 = new TestTransaction(config, new ArrayByteSequence("url0000"), new Column("attr", "lastupdate"));
    tx5.sete("idx:abc", new Column("doc", "url")).from("url0000");
    tx5.sete("idx:def", new Column("doc", "url")).from("url0000");
    cd = tx5.createCommitData();
    Assert.assertTrue(tx5.preCommit(cd, new ArrayByteSequence("idx:abc"), new Column("doc", "url")));
    long commitTs = OracleClient.getInstance(config).getTimestamp();
    Assert.assertTrue(tx5.commitPrimaryColumn(cd, commitTs));
    
    // should roll tx5 forward
    TestTransaction tx6 = new TestTransaction(config);
    Assert.assertEquals("3", tx6.getd("url0000", new Column("attr", "lastupdate")).toString());
    Assert.assertEquals("url0000", tx6.getd("idx:abc", new Column("doc", "url")).toString());
    Assert.assertEquals("url0000", tx6.getd("idx:def", new Column("doc", "url")).toString());
    
    iter = scanner.iterator();
    Assert.assertFalse(iter.hasNext());

    // TODO is tx4 start before tx5, then this test will not work because AlreadyAck is not thrown for overlapping.. CommitException is thrown
    TestTransaction tx4 = new TestTransaction(config, new ArrayByteSequence("url0000"), new Column("attr", "lastupdate"));
    tx4.sete("idx:abc", new Column("doc", "url")).from("url0000");
    tx4.sete("idx:def", new Column("doc", "url")).from("url0000");

    try {
      // should not go through if tx5 is properly rolled forward
      tx4.commit();
      Assert.fail();
    } catch (AlreadyAcknowledgedException aae) {}


  }
  
  @Test
  public void testStaleScan() throws Exception {
    
    TestTransaction tx = new TestTransaction(config);
    
    tx.sete("bob", balanceCol).from("10");
    tx.sete("joe", balanceCol).from("20");
    tx.sete("jill", balanceCol).from("60");
    
    tx.commit();
    
    TestTransaction tx2 = new TestTransaction(config);
    Assert.assertEquals("10", tx2.getd("bob", balanceCol).toString());
    
    transfer(config, "joe", "jill", 1);
    transfer(config, "joe", "bob", 1);
    transfer(config, "bob", "joe", 2);
    transfer(config, "jill", "joe", 2);
    
    conn.tableOperations().flush(table, null, null, true);
    
    try {
      tx2.getd("joe", balanceCol);
      Assert.assertFalse(true);
    } catch (StaleScanException sse) {
      
    }
    
    TestTransaction tx3 = new TestTransaction(config);
    
    Assert.assertEquals("9", tx3.getd("bob", balanceCol).toString());
    Assert.assertEquals("22", tx3.getd("joe", balanceCol).toString());
    Assert.assertEquals("59", tx3.getd("jill", balanceCol).toString());
  }
  
  @Test
  public void testCommitBug1() throws Exception {
    
    TestTransaction tx1 = new TestTransaction(config);
    
    tx1.sete("bob", balanceCol).from("10");
    tx1.sete("joe", balanceCol).from("20");
    tx1.sete("jill", balanceCol).from("60");
    
    CommitData cd = tx1.createCommitData();
    Assert.assertTrue(tx1.preCommit(cd));
    
    while (true) {
      TestTransaction tx2 = new TestTransaction(config);
    
      tx2.sete("bob", balanceCol).from("11");
      tx2.sete("jill", balanceCol).from("61");
      
      // tx1 should be rolled back even in case where columns tx1 locked are not read by tx2
      try {
        tx2.commit();
        break;
      } catch (CommitException ce) {
      
      }
    }

    TestTransaction tx4 = new TestTransaction(config);
    
    Assert.assertEquals("11", tx4.getd("bob", balanceCol).toString());
    Assert.assertNull(tx4.getd("joe", balanceCol).toString());
    Assert.assertEquals("61", tx4.getd("jill", balanceCol).toString());
  }
}
