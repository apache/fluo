/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.integration.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;

import com.google.common.collect.Sets;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.impl.TransactorNode;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

public class ParallelScannerIT extends ITBaseImpl {

  @Test
  public void testRowColumn() {
    TestTransaction tx1 = new TestTransaction(env);

    tx1.set("node1", new Column("edge", "node2"), "");
    tx1.set("node1", new Column("edge", "node3"), "");
    tx1.set("node3", new Column("edge", "node4"), "");
    tx1.set("node5", new Column("edge", "node7"), "");
    tx1.set("node5", new Column("edge", "node2"), "");
    tx1.set("node5", new Column("edge", "node8"), "");

    tx1.done();

    TestTransaction tx2 = new TestTransaction(env);

    ArrayList<RowColumn> newEdges = new ArrayList<>();

    newEdges.add(new RowColumn("node1", new Column("edge", "node3")));
    newEdges.add(new RowColumn("node5", new Column("edge", "node2")));
    newEdges.add(new RowColumn("node5", new Column("edge", "node9")));
    newEdges.add(new RowColumn("node1", new Column("edge", "node8")));
    newEdges.add(new RowColumn("node8", new Column("edge", "node3")));
    newEdges.add(new RowColumn("node5", new Column("edge", "node7")));

    Map<RowColumn, String> existing = tx2.gets(newEdges);

    tx2.done();

    HashSet<RowColumn> expected = new HashSet<>();
    expected.add(new RowColumn("node1", new Column("edge", "node3")));
    expected.add(new RowColumn("node5", new Column("edge", "node2")));
    expected.add(new RowColumn("node5", new Column("edge", "node7")));

    Assert.assertEquals(expected, existing.keySet());
  }

  @Test
  public void testConcurrentParallelScan() throws Exception {
    // have one transaction lock a row/cole and another attempt to read that row/col as part of a
    // parallel scan
    TestTransaction tx1 = new TestTransaction(env);

    tx1.set("bob9", new Column("vote", "election1"), "N");
    tx1.set("bob9", new Column("vote", "election2"), "Y");

    tx1.set("joe3", new Column("vote", "election1"), "nay");
    tx1.set("joe3", new Column("vote", "election2"), "nay");

    tx1.done();

    final TestTransaction tx2 = new TestTransaction(env);

    tx2.set("sue4", new Column("vote", "election1"), "+1");
    tx2.set("sue4", new Column("vote", "election2"), "-1");

    tx2.set("eve2", new Column("vote", "election1"), "no");
    tx2.set("eve2", new Column("vote", "election2"), "no");

    final CommitData cd2 = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd2));
    final Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    Assert.assertTrue(tx2.commitPrimaryColumn(cd2, commitTs));

    // create a thread that will unlock column while transaction tx3 is executing

    Runnable finishCommitTask = new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(500);
          tx2.finishCommit(cd2, commitTs);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };

    Thread commitThread = new Thread(finishCommitTask);
    commitThread.start();

    TestTransaction tx3 = new TestTransaction(env);

    Column e1Col = new Column("vote", "election1");

    // normally when this test runs, some of the row/columns being read below will be locked for a
    // bit
    Map<String, Map<Column, String>> votes =
        tx3.gets(Arrays.asList("bob9", "joe3", "sue4", "eve2"), Sets.newHashSet(e1Col));

    Assert.assertEquals("N", votes.get("bob9").get(e1Col));
    Assert.assertEquals("nay", votes.get("joe3").get(e1Col));
    Assert.assertEquals("+1", votes.get("sue4").get(e1Col));
    Assert.assertEquals("no", votes.get("eve2").get(e1Col));
    Assert.assertEquals(4, votes.size());
  }

  @Test
  public void testParallelScanRecovery1() throws Exception {
    runParallelRecoveryTest(true);
  }

  @Test
  public void testParallelScanRecovery2() throws Exception {
    runParallelRecoveryTest(false);
  }

  private static final Column COL = new Column("7", "7");

  private void runParallelRecoveryTest(boolean closeTransID) throws Exception {
    TestTransaction tx1 = new TestTransaction(env);

    tx1.set("5", COL, "3");
    tx1.set("12", COL, "10");
    tx1.set("19", COL, "17");
    tx1.set("26", COL, "24");
    tx1.set("33", COL, "31");
    tx1.set("40", COL, "38");
    tx1.set("47", COL, "45");

    tx1.done();

    TransactorNode tNode1 = new TransactorNode(env);

    TestTransaction tx2 = new TestTransaction(env, tNode1);

    tx2.set("5", COL, "7");
    tx2.set("12", COL, "14");
    tx2.set("19", COL, "21");

    CommitData cd2 = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd2));

    TestTransaction tx3 = new TestTransaction(env, tNode1);

    tx3.set("26", COL, "28");
    tx3.set("33", COL, "35");
    tx3.set("40", COL, "42");

    CommitData cd3 = tx3.createCommitData();
    Assert.assertTrue(tx3.preCommit(cd3));
    Stamp commitTs = env.getSharedResources().getOracleClient().getStamp();
    tx3.commitPrimaryColumn(cd3, commitTs);

    if (closeTransID) {
      tNode1.close();
    }

    check();
    check();

    if (!closeTransID) {
      tNode1.close();
    }
  }

  private void check() throws Exception {
    TestTransaction tx = new TestTransaction(env);
    Map<String, Map<Column, String>> votes =
        tx.gets(Arrays.asList("5", "12", "19", "26", "33", "40", "47"), Sets.newHashSet(COL));

    // following should be rolled back
    Assert.assertEquals("3", votes.get("5").get(COL));
    Assert.assertEquals("10", votes.get("12").get(COL));
    Assert.assertEquals("17", votes.get("19").get(COL));

    // following should be rolled forward
    Assert.assertEquals("28", votes.get("26").get(COL));
    Assert.assertEquals("35", votes.get("33").get(COL));
    Assert.assertEquals("42", votes.get("40").get(COL));

    // unchanged and not locked
    Assert.assertEquals("45", votes.get("47").get(COL));
  }

}
