/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.fluo.core.impl;

import java.util.Arrays;
import java.util.Map;

import io.fluo.api.data.Column;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedSnapshotBase.Value;
import io.fluo.core.ITBaseImpl;
import io.fluo.core.TestTransaction;
import io.fluo.core.impl.TransactionImpl.CommitData;
import org.junit.Assert;
import org.junit.Test;

public class ParallelScannerIT extends ITBaseImpl {
  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  @Test
  public void testConcurrentParallelScan() throws Exception {
    // have one transaction lock a row/cole and another attempt to read that row/col as part of a
    // parallel scan
    TestTransaction tx1 = new TestTransaction(env);

    tx1.mutate().row("bob9").fam("vote").qual("election1").set("N");
    tx1.mutate().row("bob9").fam("vote").qual("election2").set("Y");

    tx1.mutate().row("joe3").fam("vote").qual("election1").set("nay");
    tx1.mutate().row("joe3").fam("vote").qual("election2").set("nay");

    tx1.done();

    final TestTransaction tx2 = new TestTransaction(env);

    tx2.mutate().row("sue4").fam("vote").qual("election1").set("+1");
    tx2.mutate().row("sue4").fam("vote").qual("election2").set("-1");

    tx2.mutate().row("eve2").fam("vote").qual("election1").set("no");
    tx2.mutate().row("eve2").fam("vote").qual("election2").set("no");

    final CommitData cd2 = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd2));
    final long commitTs = env.getSharedResources().getOracleClient().getTimestamp();
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

    Column e1Col = typeLayer.bc().fam("vote").qual("election1").vis();

    // normally when this test runs, some of the row/columns being read below will be locked for a
    // bit
    Map<String, Map<Column, Value>> votes =
        tx3.get().rowsString(Arrays.asList("bob9", "joe3", "sue4", "eve2")).columns(e1Col)
            .toStringMap();

    Assert.assertEquals("N", votes.get("bob9").get(e1Col).toString(""));
    Assert.assertEquals("nay", votes.get("joe3").get(e1Col).toString(""));
    Assert.assertEquals("+1", votes.get("sue4").get(e1Col).toString(""));
    Assert.assertEquals("no", votes.get("eve2").get(e1Col).toString(""));
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

  void runParallelRecoveryTest(boolean closeTransID) throws Exception {
    TestTransaction tx1 = new TestTransaction(env);

    tx1.mutate().row(5).fam(7).qual(7).set(3);
    tx1.mutate().row(12).fam(7).qual(7).set(10);
    tx1.mutate().row(19).fam(7).qual(7).set(17);
    tx1.mutate().row(26).fam(7).qual(7).set(24);
    tx1.mutate().row(33).fam(7).qual(7).set(31);
    tx1.mutate().row(40).fam(7).qual(7).set(38);
    tx1.mutate().row(47).fam(7).qual(7).set(45);

    tx1.done();

    TransactorNode tNode1 = new TransactorNode(env);

    TestTransaction tx2 = new TestTransaction(env, tNode1);

    tx2.mutate().row(5).fam(7).qual(7).set(7);
    tx2.mutate().row(12).fam(7).qual(7).set(14);
    tx2.mutate().row(19).fam(7).qual(7).set(21);

    CommitData cd2 = tx2.createCommitData();
    Assert.assertTrue(tx2.preCommit(cd2));

    TestTransaction tx3 = new TestTransaction(env, tNode1);

    tx3.mutate().row(26).fam(7).qual(7).set(28);
    tx3.mutate().row(33).fam(7).qual(7).set(35);
    tx3.mutate().row(40).fam(7).qual(7).set(42);

    CommitData cd3 = tx3.createCommitData();
    Assert.assertTrue(tx3.preCommit(cd3));
    long commitTs = env.getSharedResources().getOracleClient().getTimestamp();
    tx3.commitPrimaryColumn(cd3, commitTs);

    if (closeTransID)
      tNode1.close();

    check();
    check();

    if (!closeTransID)
      tNode1.close();
  }

  void check() throws Exception {
    TestTransaction tx = new TestTransaction(env);
    Column scol = typeLayer.bc().fam(7).qual(7).vis();
    Map<String, Map<Column, Value>> votes =
        tx.get().rowsString(Arrays.asList("5", "12", "19", "26", "33", "40", "47")).columns(scol)
            .toStringMap();

    // following should be rolled back
    Assert.assertEquals(3, votes.get("5").get(scol).toInteger(0));
    Assert.assertEquals(10, votes.get("12").get(scol).toInteger(0));
    Assert.assertEquals(17, votes.get("19").get(scol).toInteger(0));

    // following should be rolled forward
    Assert.assertEquals(28, votes.get("26").get(scol).toInteger(0));
    Assert.assertEquals(35, votes.get("33").get(scol).toInteger(0));
    Assert.assertEquals(42, votes.get("40").get(scol).toInteger(0));

    // unchanged and not locked
    Assert.assertEquals(45, votes.get("47").get(scol).toInteger(0));
  }

}
