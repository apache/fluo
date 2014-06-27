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
package org.fluo.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import org.fluo.api.Column;
import org.fluo.api.ColumnIterator;
import org.fluo.api.Observer;
import org.fluo.api.RowIterator;
import org.fluo.api.ScannerConfiguration;
import org.fluo.api.Transaction;
import org.fluo.api.config.ObserverConfiguration;
import org.fluo.api.types.StringEncoder;
import org.fluo.api.types.TypeLayer;
import org.fluo.api.types.TypedTransaction;
import org.fluo.impl.TransactionImpl.CommitData;

/**
 * A simple test that added links between nodes in a graph.  There is an observer
 * that updates an index of node degree.
 */
public class WorkerIT extends Base {
  
  private static final ByteSequence NODE_CF = new ArrayByteSequence("node");

  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  protected Map<Column,ObserverConfiguration> getObservers() {
    Map<Column,ObserverConfiguration> observed = new HashMap<Column,ObserverConfiguration>();
    observed.put(typeLayer.newColumn("attr", "lastupdate"), new ObserverConfiguration(DegreeIndexer.class.getName()));
    return observed;
  }

  static class DegreeIndexer implements Observer {
    
    @Override
    public void init(Map<String,String> config) {}

    public void process(Transaction tx, ByteSequence row, Column col) throws Exception {
      // get previously calculated degree
      
      ByteSequence degree = tx.get(row, typeLayer.newColumn("attr", "degree"));
      TypedTransaction ttx = typeLayer.transaction(tx);

      // calculate new degree
      int count = 0;
      RowIterator riter = ttx.get(new ScannerConfiguration().setRange(Range.exact(new Text(row.toArray()), new Text("link"))));
      while (riter.hasNext()) {
        ColumnIterator citer = riter.next().getValue();
        while (citer.hasNext()) {
          citer.next();
          count++;
        }
      }
      String degree2 = "" + count;
      
      if (degree == null || !degree.toString().equals(degree2)) {
        ttx.set(row, typeLayer.newColumn("attr", "degree"), new ArrayByteSequence(degree2));
        
        // put new entry in degree index
        ttx.mutate().row("IDEG" + degree2).col(new Column(NODE_CF, row)).set("");
      }
      
      if (degree != null) {
        // delete old degree in index
        ttx.mutate().row("IDEG" + degree).col(new Column(NODE_CF, row)).delete();
      }
    }

    @Override
    public void close() {}
  }
  
  
  
  @Test
  public void test1() throws Exception {
    
    TestTransaction tx1 = new TestTransaction(config);

    //add a link between two nodes in a graph    
    tx1.mutate().row("N0003").col(typeLayer.newColumn("link", "N0040")).set("");
    tx1.mutate().row("N0003").col(typeLayer.newColumn("attr", "lastupdate")).set(System.currentTimeMillis() + "");
    
    tx1.commit();
    
    TestTransaction tx2 = new TestTransaction(config);
    
    //add a link between two nodes in a graph    
    tx2.mutate().row("N0003").col(typeLayer.newColumn("link", "N0020")).set("");
    tx2.mutate().row("N0003").col(typeLayer.newColumn("attr", "lastupdate")).set(System.currentTimeMillis() + "");
    
    tx2.commit();
    
    runWorker();
   
    //verify observer updated degree index 
    TestTransaction tx3 = new TestTransaction(config);
    Assert.assertEquals("2", tx3.get().row("N0003").col(typeLayer.newColumn("attr", "degree")).toString());
    Assert.assertEquals("", tx3.get().row("IDEG2").col(typeLayer.newColumn("node", "N0003")).toString());
    
    //add a link between two nodes in a graph    
    tx3.mutate().row("N0003").col(typeLayer.newColumn("link", "N0010")).set("");
    tx3.mutate().row("N0003").col(typeLayer.newColumn("attr", "lastupdate")).set(System.currentTimeMillis() + "");
    tx3.commit();
    
    runWorker();
    
    //verify observer updated degree index.  Should have deleted old index entry 
    //and added a new one 
    TestTransaction tx4 = new TestTransaction(config);
    Assert.assertEquals("3", tx4.get().row("N0003").col(typeLayer.newColumn("attr", "degree")).toString());
    Assert.assertNull("", tx4.get().row("IDEG2").col(typeLayer.newColumn("node", "N0003")).toString());
    Assert.assertEquals("", tx4.get().row("IDEG3").col(typeLayer.newColumn("node", "N0003")).toString());
    
    // test rollback
    TestTransaction tx5 = new TestTransaction(config);
    tx5.mutate().row("N0003").col(typeLayer.newColumn("link", "N0030")).set("");
    tx5.mutate().row("N0003").col(typeLayer.newColumn("attr", "lastupdate")).set(System.currentTimeMillis() + "");
    tx5.commit();
    
    TestTransaction tx6 = new TestTransaction(config);
    tx6.mutate().row("N0003").col(typeLayer.newColumn("link", "N0050")).set("");
    tx6.mutate().row("N0003").col(typeLayer.newColumn("attr", "lastupdate")).set(System.currentTimeMillis() + "");
    CommitData cd = tx6.createCommitData();
    tx6.preCommit(cd, new ArrayByteSequence("N0003"), typeLayer.newColumn("attr", "lastupdate"));

    runWorker();
    
    TestTransaction tx7 = new TestTransaction(config);
    Assert.assertEquals("4", tx7.get().row("N0003").col(typeLayer.newColumn("attr", "degree")).toString());
    Assert.assertNull("", tx7.get().row("IDEG3").col(typeLayer.newColumn("node", "N0003")).toString());
    Assert.assertEquals("", tx7.get().row("IDEG4").col(typeLayer.newColumn("node", "N0003")).toString());
  }
  
  // TODO test that observers trigger on delete
}
