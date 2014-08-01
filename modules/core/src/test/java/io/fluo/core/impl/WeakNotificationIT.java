package io.fluo.core.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import io.fluo.api.client.Transaction;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedTransaction;
import io.fluo.core.TestBaseImpl;
import io.fluo.core.TestTransaction;
import io.fluo.core.impl.TransactionImpl.CommitData;
import io.fluo.core.oracle.OracleClient;
import org.junit.Assert;
import org.junit.Test;

public class WeakNotificationIT extends TestBaseImpl {

  private static TypeLayer tl = new TypeLayer(new StringEncoder());

  public static class SimpleObserver extends AbstractObserver {

    @Override
    public void process(Transaction tx, Bytes row, Column col) throws Exception {
      TypedTransaction ttx = tl.transaction(tx);

      ScannerConfiguration sc = new ScannerConfiguration();
      sc.setSpan(Span.exact(row, Bytes.wrap("stats")));
      RowIterator rowIter = ttx.get(sc);

      int sum = 0;

      if (rowIter.hasNext()) {
        ColumnIterator colIter = rowIter.next().getValue();
        while (colIter.hasNext()) {
          Entry<Column,Bytes> colVal = colIter.next();
          sum += Integer.parseInt(colVal.getValue().toString());
          ttx.delete(row, colVal.getKey());
        }
      }

      if (sum != 0) {
        sum += ttx.get().row(row).fam("stat").qual("count").toInteger(0);
        ttx.mutate().row(row).fam("stat").qual("count").set(sum);
      }
    }

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(tl.newColumn("stat", "check"), NotificationType.WEAK);
    }
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(SimpleObserver.class.getName()));
  }

  @Test
  public void testWeakNotification() throws Exception {
    TestTransaction tx1 = new TestTransaction(env);
    tx1.mutate().row("r1").fam("stat").qual("count").set(3);
    tx1.commit();

    TestTransaction tx2 = new TestTransaction(env);
    tx2.mutate().row("r1").fam("stats").qual("af89").set(5);
    tx2.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
    tx2.commit();

    TestTransaction tx3 = new TestTransaction(env);
    tx3.mutate().row("r1").fam("stats").qual("af99").set(7);
    tx3.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
    tx3.commit();

    runWorker();

    TestTransaction tx4 = new TestTransaction(env);
    Assert.assertEquals(15, tx4.get().row("r1").fam("stat").qual("count").toInteger(0));

    // overlapping transactions that set a weak notification should commit w/ no problem
    TestTransaction tx5 = new TestTransaction(env);
    tx5.mutate().row("r1").fam("stats").qual("bff7").set(11);
    tx5.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
    CommitData cd5 = tx5.createCommitData();
    Assert.assertTrue(tx5.preCommit(cd5));

    TestTransaction tx6 = new TestTransaction(env);
    tx6.mutate().row("r1").fam("stats").qual("bff0").set(13);
    tx6.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
    CommitData cd6 = tx6.createCommitData();
    Assert.assertTrue(tx6.preCommit(cd6));

    long commitTs5 = OracleClient.getInstance(env).getTimestamp();
    Assert.assertTrue(tx5.commitPrimaryColumn(cd5, commitTs5));

    long commitTs6 = OracleClient.getInstance(env).getTimestamp();
    Assert.assertTrue(tx6.commitPrimaryColumn(cd6, commitTs6));

    tx6.finishCommit(cd6, commitTs6);
    tx5.finishCommit(cd5, commitTs5);

    runWorker();

    TestTransaction tx7 = new TestTransaction(env);
    Assert.assertEquals(39, tx7.get().row("r1").fam("stat").qual("count").toInteger(0));
  }

  @Test(timeout = 30000)
  public void testNOOP() throws Exception {
    // if an observer makes not updates in a transaction, it should still delete the weak notification
    TestTransaction tx1 = new TestTransaction(env);
    tx1.mutate().row("r1").fam("stat").qual("count").set(3);
    tx1.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
    tx1.commit();

    // the following will loop forever if weak notification is not deleted
    runWorker();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadColumn() throws Exception {
    TestTransaction tx1 = new TestTransaction(env);
    tx1.mutate().row("r1").fam("stat").qual("count").set(3);
    tx1.mutate().row("r1").fam("stat").qual("foo").weaklyNotify();
    tx1.commit();
  }

}
