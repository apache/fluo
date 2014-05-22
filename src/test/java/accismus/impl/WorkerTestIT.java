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
import accismus.api.types.TypedTransaction;
import accismus.impl.TransactionImpl.CommitData;

/**
 * A simple test that added links between nodes in a graph.  There is an observer
 * that updates an index of node degree.
 */
public class WorkerTestIT extends Base {
  
  private static final ByteSequence NODE_CF = new ArrayByteSequence("node");

  protected Map<Column,String> getObservers() {
    Map<Column,String> observed = new HashMap<Column,String>();
    observed.put(new Column("attr", "lastupdate"), DegreeIndexer.class.getName());
    return observed;
  }

  static class DegreeIndexer implements Observer {
    
    public void process(Transaction tx, ByteSequence row, Column col) throws Exception {
      // get previously calculated degree
      
      ByteSequence degree = tx.get(row, new Column("attr", "degree"));
      TypedTransaction ttx = new TypedTransaction(tx, new StringEncoder());

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
        ttx.set(row, new Column("attr", "degree"), new ArrayByteSequence(degree2));
        
        // put new entry in degree index
        ttx.sete("IDEG" + degree2, new Column(NODE_CF, row)).from("");
      }
      
      if (degree != null) {
        // delete old degree in index
        ttx.delete("IDEG" + degree, new Column(NODE_CF, row));
      }
    }
  }
  
  
  
  @Test
  public void test1() throws Exception {
    
    TestTransaction tx1 = new TestTransaction(config);

    //add a link between two nodes in a graph    
    tx1.sete("N0003", new Column("link", "N0040")).from("");
    tx1.sete("N0003", new Column("attr", "lastupdate")).from(System.currentTimeMillis() + "");
    
    tx1.commit();
    
    TestTransaction tx2 = new TestTransaction(config);
    
    //add a link between two nodes in a graph    
    tx2.sete("N0003", new Column("link", "N0020")).from("");
    tx2.sete("N0003", new Column("attr", "lastupdate")).from(System.currentTimeMillis() + "");
    
    tx2.commit();
    
    runWorker();
   
    //verify observer updated degree index 
    TestTransaction tx3 = new TestTransaction(config);
    Assert.assertEquals("2", tx3.getd("N0003", new Column("attr", "degree")).toString());
    Assert.assertEquals("", tx3.getd("IDEG2", new Column("node", "N0003")).toString());
    
    //add a link between two nodes in a graph    
    tx3.sete("N0003", new Column("link", "N0010")).from("");
    tx3.sete("N0003", new Column("attr", "lastupdate")).from(System.currentTimeMillis() + "");
    tx3.commit();
    
    runWorker();
    
    //verify observer updated degree index.  Should have deleted old index entry 
    //and added a new one 
    TestTransaction tx4 = new TestTransaction(config);
    Assert.assertEquals("3", tx4.getd("N0003", new Column("attr", "degree")).toString());
    Assert.assertNull("", tx4.getd("IDEG2", new Column("node", "N0003")));
    Assert.assertEquals("", tx4.getd("IDEG3", new Column("node", "N0003")).toString());
    
    // test rollback
    TestTransaction tx5 = new TestTransaction(config);
    tx5.sete("N0003", new Column("link", "N0030")).from("");
    tx5.sete("N0003", new Column("attr", "lastupdate")).from(System.currentTimeMillis() + "");
    tx5.commit();
    
    TestTransaction tx6 = new TestTransaction(config);
    tx6.sete("N0003", new Column("link", "N0050")).from("");
    tx6.sete("N0003", new Column("attr", "lastupdate")).from(System.currentTimeMillis() + "");
    CommitData cd = tx6.createCommitData();
    tx6.preCommit(cd, new ArrayByteSequence("N0003"), new Column("attr", "lastupdate"));

    runWorker();
    
    TestTransaction tx7 = new TestTransaction(config);
    Assert.assertEquals("4", tx7.getd("N0003", new Column("attr", "degree")).toString());
    Assert.assertNull("", tx7.getd("IDEG3", new Column("node", "N0003")));
    Assert.assertEquals("", tx7.getd("IDEG4", new Column("node", "N0003")).toString());
  }
  
  // TODO test that observers trigger on delete
}
