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
import java.util.Map;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import accismus.api.Column;
import accismus.api.ColumnIterator;
import accismus.api.Observer;
import accismus.api.RowIterator;
import accismus.api.ScannerConfiguration;
import accismus.api.Transaction;
import accismus.api.types.StringEncoder;
import accismus.api.types.TypeLayer;
import accismus.api.types.TypedTransaction;
import accismus.impl.TransactionImpl.CommitData;

/**
 * A simple test that added links between nodes in a graph.  There is an observer
 * that updates an index of node degree.
 */
public class WorkerIT extends Base {
  
  private static final ByteSequence NODE_CF = new ArrayByteSequence("node");

  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  protected Map<Column,String> getObservers() {
    Map<Column,String> observed = new HashMap<Column,String>();
    observed.put(typeLayer.newColumn("attr", "lastupdate"), DegreeIndexer.class.getName());
    return observed;
  }

  static class DegreeIndexer implements Observer {
    
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
        ttx.set().row("IDEG" + degree2).col(new Column(NODE_CF, row)).val("");
      }
      
      if (degree != null) {
        // delete old degree in index
        ttx.delete().row("IDEG" + degree).col(new Column(NODE_CF, row));
      }
    }
  }
  
  
  
  @Test
  public void test1() throws Exception {
    
    TestTransaction tx1 = new TestTransaction(config);

    //add a link between two nodes in a graph    
    tx1.set().row("N0003").col(typeLayer.newColumn("link", "N0040")).val("");
    tx1.set().row("N0003").col(typeLayer.newColumn("attr", "lastupdate")).val(System.currentTimeMillis() + "");
    
    tx1.commit();
    
    TestTransaction tx2 = new TestTransaction(config);
    
    //add a link between two nodes in a graph    
    tx2.set().row("N0003").col(typeLayer.newColumn("link", "N0020")).val("");
    tx2.set().row("N0003").col(typeLayer.newColumn("attr", "lastupdate")).val(System.currentTimeMillis() + "");
    
    tx2.commit();
    
    runWorker();
   
    //verify observer updated degree index 
    TestTransaction tx3 = new TestTransaction(config);
    Assert.assertEquals("2", tx3.get().row("N0003").col(typeLayer.newColumn("attr", "degree")).toString());
    Assert.assertEquals("", tx3.get().row("IDEG2").col(typeLayer.newColumn("node", "N0003")).toString());
    
    //add a link between two nodes in a graph    
    tx3.set().row("N0003").col(typeLayer.newColumn("link", "N0010")).val("");
    tx3.set().row("N0003").col(typeLayer.newColumn("attr", "lastupdate")).val(System.currentTimeMillis() + "");
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
    tx5.set().row("N0003").col(typeLayer.newColumn("link", "N0030")).val("");
    tx5.set().row("N0003").col(typeLayer.newColumn("attr", "lastupdate")).val(System.currentTimeMillis() + "");
    tx5.commit();
    
    TestTransaction tx6 = new TestTransaction(config);
    tx6.set().row("N0003").col(typeLayer.newColumn("link", "N0050")).val("");
    tx6.set().row("N0003").col(typeLayer.newColumn("attr", "lastupdate")).val(System.currentTimeMillis() + "");
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
