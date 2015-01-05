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
import io.fluo.api.types.TypedSnapshot;
import io.fluo.api.types.TypedTransaction;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.core.ITBaseMini;
import io.fluo.core.TestTransaction;
import io.fluo.core.impl.TransactionImpl.CommitData;
import io.fluo.core.mini.MiniFluoImpl;
import io.fluo.core.worker.NotificationFinder;
import io.fluo.core.worker.Observers;
import io.fluo.core.worker.finder.hash.HashNotificationFinder;
import org.junit.Assert;
import org.junit.Test;

/**
 * A simple test that added links between nodes in a graph. There is an observer that updates an index of node degree.
 */
public class WorkerIT extends ITBaseMini {

  private static final Bytes NODE_CF = Bytes.of("node");

  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  private static Column observedColumn = typeLayer.bc().fam("attr").qual("lastupdate").vis();

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(DegreeIndexer.class.getName()));
  }

  public static class DegreeIndexer implements Observer {

    @Override
    public void init(Context context) {}

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
      // get previously calculated degree

      Bytes degree = tx.get(row, typeLayer.bc().fam("attr").qual("degree").vis());
      TypedTransactionBase ttx = typeLayer.wrap(tx);

      // calculate new degree
      int count = 0;
      RowIterator riter = ttx.get(new ScannerConfiguration().setSpan(Span.exact(row, new Column(Bytes.of("link")))));
      while (riter.hasNext()) {
        ColumnIterator citer = riter.next().getValue();
        while (citer.hasNext()) {
          citer.next();
          count++;
        }
      }
      String degree2 = "" + count;

      if (degree == null || !degree.toString().equals(degree2)) {
        ttx.set(row, typeLayer.bc().fam("attr").qual("degree").vis(), Bytes.of(degree2));

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

    Environment env = new Environment(config);
  
    addLink("N0003", "N0040");
    addLink("N0003", "N0020");

    miniFluo.waitForObservers();

    // verify observer updated degree index
    TestTransaction tx3 = new TestTransaction(env);
    Assert.assertEquals("2", tx3.get().row("N0003").fam("attr").qual("degree").toString());
    Assert.assertEquals("", tx3.get().row("IDEG2").fam("node").qual("N0003").toString());

    // add a link between two nodes in a graph
    tx3.mutate().row("N0003").fam("link").qual("N0010").set("");
    tx3.mutate().row("N0003").fam("attr").qual("lastupdate").set(System.currentTimeMillis() + "");
    tx3.done();

    miniFluo.waitForObservers();

    // verify observer updated degree index. Should have deleted old index entry
    // and added a new one
    TestTransaction tx4 = new TestTransaction(env);
    Assert.assertEquals("3", tx4.get().row("N0003").fam("attr").qual("degree").toString());
    Assert.assertNull("", tx4.get().row("IDEG2").fam("node").qual("N0003").toString());
    Assert.assertEquals("", tx4.get().row("IDEG3").fam("node").qual("N0003").toString());

    // test rollback
    TestTransaction tx5 = new TestTransaction(env);
    tx5.mutate().row("N0003").fam("link").qual("N0030").set("");
    tx5.mutate().row("N0003").fam("attr").qual("lastupdate").set(System.currentTimeMillis() + "");
    tx5.done();

    TestTransaction tx6 = new TestTransaction(env);
    tx6.mutate().row("N0003").fam("link").qual("N0050").set("");
    tx6.mutate().row("N0003").fam("attr").qual("lastupdate").set(System.currentTimeMillis() + "");
    CommitData cd = tx6.createCommitData();
    tx6.preCommit(cd, Bytes.of("N0003"), typeLayer.bc().fam("attr").qual("lastupdate").vis());

    miniFluo.waitForObservers();

    TestTransaction tx7 = new TestTransaction(env);
    Assert.assertEquals("4", tx7.get().row("N0003").fam("attr").qual("degree").toString());
    Assert.assertNull("", tx7.get().row("IDEG3").fam("node").qual("N0003").toString());
    Assert.assertEquals("", tx7.get().row("IDEG4").fam("node").qual("N0003").toString());
    
    env.close();
  }

  /*
   * test when the configured column of an observer stored in zk differs from what the class returns
   */
  @Test
  public void testDiffObserverConfig() throws Exception {
    Column old = observedColumn;
    observedColumn = typeLayer.bc().fam("attr2").qual("lastupdate").vis();
    try {
      try(Environment env = new Environment(config); Observers observers = new Observers(env)){
        observers.getObserver(typeLayer.bc().fam("attr").qual("lastupdate").vis());
      }

      Assert.fail();

    } catch (IllegalStateException ise) {
      Assert.assertTrue(ise.getMessage().contains("Mismatch between configured column and class column"));
    } finally {
      observedColumn = old;
    }
  }

  private void addLink(String from, String to){
    try(TypedTransaction tx = typeLayer.wrap(client.newTransaction())){
      tx.mutate().row(from).fam("link").qual(to).set("");
      tx.mutate().row(from).fam("attr").qual("lastupdate").set(System.currentTimeMillis() + "");
      tx.commit();
    }
  }
  
  @Test
  public void testMultipleFinders(){
    
    try(Environment env = new Environment(config)){
    
      NotificationFinder nf1 = new HashNotificationFinder();
      nf1.init(env, ((MiniFluoImpl)miniFluo).getNotificationProcessor());
      nf1.start();
      
      NotificationFinder nf2 = new HashNotificationFinder();
      nf2.init(env, ((MiniFluoImpl)miniFluo).getNotificationProcessor());
      nf2.start();
      
      for(int i = 0; i< 10; i++){
        addLink("N0003", "N00"+i+"0");
      }
      
      miniFluo.waitForObservers();
      
      try(TypedSnapshot snap = typeLayer.wrap(client.newSnapshot())){
        Assert.assertEquals("10", snap.get().row("N0003").fam("attr").qual("degree").toString());
        Assert.assertEquals("", snap.get().row("IDEG10").fam("node").qual("N0003").toString());
      }
      
      nf2.stop();
      
      for(int i = 1; i< 10; i++){
        addLink("N0003", "N0"+i+"00");
      }
      
      miniFluo.waitForObservers();
      
      try(TypedSnapshot snap = typeLayer.wrap(client.newSnapshot())){
        Assert.assertEquals("19", snap.get().row("N0003").fam("attr").qual("degree").toString());
        Assert.assertEquals("", snap.get().row("IDEG19").fam("node").qual("N0003").toString());
        Assert.assertNull(snap.get().row("IDEG10").fam("node").qual("N0003").toString());
      }
      
      nf1.stop();
      
    }
    
    
    
  }
  
  
  // TODO test that observers trigger on delete
}
