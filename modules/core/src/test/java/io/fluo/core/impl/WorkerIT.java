/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.core.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.observer.Observer;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.core.TestBaseImpl;
import io.fluo.core.TestTransaction;
import io.fluo.core.impl.TransactionImpl.CommitData;
import org.junit.Assert;
import org.junit.Test;

/**
 * A simple test that added links between nodes in a graph. There is an observer that updates an index of node degree.
 */
public class WorkerIT extends TestBaseImpl {

  private static final Bytes NODE_CF = Bytes.wrap("node");

  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  private static Column observedColumn = typeLayer.bc().fam("attr").qual("lastupdate").vis();

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(DegreeIndexer.class.getName()));
  }

  public static class DegreeIndexer implements Observer {

    @Override
    public void init(Map<String,String> config) {}

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
      // get previously calculated degree

      Bytes degree = tx.get(row, typeLayer.bc().fam("attr").qual("degree").vis());
      TypedTransactionBase ttx = typeLayer.wrap(tx);

      // calculate new degree
      int count = 0;
      RowIterator riter = ttx.get(new ScannerConfiguration().setSpan(Span.exact(row, Bytes.wrap("link"))));
      while (riter.hasNext()) {
        ColumnIterator citer = riter.next().getValue();
        while (citer.hasNext()) {
          citer.next();
          count++;
        }
      }
      String degree2 = "" + count;

      if (degree == null || !degree.toString().equals(degree2)) {
        ttx.set(row, typeLayer.bc().fam("attr").qual("degree").vis(), Bytes.wrap(degree2));

        // put new entry in degree index
        ttx.mutate().row("IDEG" + degree2).col(new Column(NODE_CF, row)).set("");
      }

      if (degree != null) {
        // delete old degree in index
        ttx.mutate().row("IDEG" + degree).col(new Column(NODE_CF, row)).delete();
      }
    }

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(observedColumn, NotificationType.STRONG);
    }

    @Override
    public void close() {}
  }

  @Test
  public void test1() throws Exception {

    TestTransaction tx1 = new TestTransaction(env);

    // add a link between two nodes in a graph
    tx1.mutate().row("N0003").col(typeLayer.bc().fam("link").qual("N0040").vis()).set("");
    tx1.mutate().row("N0003").col(typeLayer.bc().fam("attr").qual("lastupdate").vis()).set(System.currentTimeMillis() + "");

    tx1.done();

    TestTransaction tx2 = new TestTransaction(env);

    // add a link between two nodes in a graph
    tx2.mutate().row("N0003").col(typeLayer.bc().fam("link").qual("N0020").vis()).set("");
    tx2.mutate().row("N0003").col(typeLayer.bc().fam("attr").qual("lastupdate").vis()).set(System.currentTimeMillis() + "");

    tx2.done();

    runWorker();

    // verify observer updated degree index
    TestTransaction tx3 = new TestTransaction(env);
    Assert.assertEquals("2", tx3.get().row("N0003").col(typeLayer.bc().fam("attr").qual("degree").vis()).toString());
    Assert.assertEquals("", tx3.get().row("IDEG2").col(typeLayer.bc().fam("node").qual("N0003").vis()).toString());

    // add a link between two nodes in a graph
    tx3.mutate().row("N0003").col(typeLayer.bc().fam("link").qual("N0010").vis()).set("");
    tx3.mutate().row("N0003").col(typeLayer.bc().fam("attr").qual("lastupdate").vis()).set(System.currentTimeMillis() + "");
    tx3.done();

    runWorker();

    // verify observer updated degree index. Should have deleted old index entry
    // and added a new one
    TestTransaction tx4 = new TestTransaction(env);
    Assert.assertEquals("3", tx4.get().row("N0003").col(typeLayer.bc().fam("attr").qual("degree").vis()).toString());
    Assert.assertNull("", tx4.get().row("IDEG2").col(typeLayer.bc().fam("node").qual("N0003").vis()).toString());
    Assert.assertEquals("", tx4.get().row("IDEG3").col(typeLayer.bc().fam("node").qual("N0003").vis()).toString());

    // test rollback
    TestTransaction tx5 = new TestTransaction(env);
    tx5.mutate().row("N0003").col(typeLayer.bc().fam("link").qual("N0030").vis()).set("");
    tx5.mutate().row("N0003").col(typeLayer.bc().fam("attr").qual("lastupdate").vis()).set(System.currentTimeMillis() + "");
    tx5.done();

    TestTransaction tx6 = new TestTransaction(env);
    tx6.mutate().row("N0003").col(typeLayer.bc().fam("link").qual("N0050").vis()).set("");
    tx6.mutate().row("N0003").col(typeLayer.bc().fam("attr").qual("lastupdate").vis()).set(System.currentTimeMillis() + "");
    CommitData cd = tx6.createCommitData();
    tx6.preCommit(cd, Bytes.wrap("N0003"), typeLayer.bc().fam("attr").qual("lastupdate").vis());

    runWorker();

    TestTransaction tx7 = new TestTransaction(env);
    Assert.assertEquals("4", tx7.get().row("N0003").col(typeLayer.bc().fam("attr").qual("degree").vis()).toString());
    Assert.assertNull("", tx7.get().row("IDEG3").col(typeLayer.bc().fam("node").qual("N0003").vis()).toString());
    Assert.assertEquals("", tx7.get().row("IDEG4").col(typeLayer.bc().fam("node").qual("N0003").vis()).toString());
  }

  /*
   * test when the configured column of an observer stored in zk differs from what the class returns
   */
  public void testDiffObserverConfig() throws Exception {
    Column old = observedColumn;
    observedColumn = typeLayer.bc().fam("attr2").qual("lastupdate").vis();
    try {

      TestTransaction tx1 = new TestTransaction(env);

      // add a link between two nodes in a graph
      tx1.mutate().row("N0003").col(typeLayer.bc().fam("link").qual("N0040").vis()).set("");
      tx1.mutate().row("N0003").col(typeLayer.bc().fam("attr").qual("lastupdate").vis()).set(System.currentTimeMillis() + "");

      tx1.done();

      runWorker();

      Assert.fail();

    } catch (IllegalStateException ise) {
      Assert.assertTrue(ise.getMessage().contains("Mismatch between configured column and class column"));
    } finally {
      observedColumn = old;
    }
  }

  // TODO test that observers trigger on delete
}
