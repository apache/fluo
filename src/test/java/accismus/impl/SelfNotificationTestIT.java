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
import org.junit.Ignore;
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
 * Test an observer notifying the column its observing.  This is a useful pattern for exporting data.
 */
public class SelfNotificationTestIT extends Base {
  
  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());
  
  static final Column STAT_COUNT_COL = typeLayer.newColumn("stat", "count");
  static final Column EXPORT_COUNT_COL = typeLayer.newColumn("export", "count");

  

  protected Map<Column,String> getObservers() {
    Map<Column,String> observed = new HashMap<Column,String>();
    observed.put(typeLayer.newColumn("export", "count"), ExportingObserver.class.getName());
    return observed;
  }

  static Integer exp = null;
  
  static class ExportingObserver implements Observer {
    
   
    
    public void process(Transaction tx, ByteSequence row, Column col) throws Exception {

      TypedTransaction ttx = typeLayer.transaction(tx);
       
      Integer currentCount = ttx.get().row(row).col(STAT_COUNT_COL).toInteger();
      Integer exportCount = ttx.get().row(row).col(EXPORT_COUNT_COL).toInteger();
      
      if(exportCount != null){
        export(row, exportCount);
        
        if(currentCount == null || exportCount.equals(currentCount)){
          ttx.delete().row(row).col(EXPORT_COUNT_COL);
        }else{
          ttx.set().row(row).col(EXPORT_COUNT_COL).val(currentCount);
        }
        
      }
    }

    private void export(ByteSequence row, Integer exportCount) {
      exp = exportCount;
    }
  }
  
  
  @Ignore
  @Test
  public void test1() throws Exception {
    
    TestTransaction tx1 = new TestTransaction(config);

    tx1.set().row("r1").col(STAT_COUNT_COL).val(3);
    tx1.set().row("r1").col(EXPORT_COUNT_COL).val(3);
    
    tx1.commit();
    
    runWorker();
    Assert.assertEquals(3, exp.intValue());
    exp = null;
    runWorker();
    Assert.assertNull(exp);
   
    TestTransaction tx2 = new TestTransaction(config);

    Assert.assertNull(tx2.get().row("r1").col(EXPORT_COUNT_COL).toInteger());
    
    tx2.set().row("r1").col(STAT_COUNT_COL).val(4);
    tx2.set().row("r1").col(EXPORT_COUNT_COL).val(4);
    
    tx2.commit();
    
    TestTransaction tx3 = new TestTransaction(config);

    tx3.set().row("r1").col(STAT_COUNT_COL).val(5);
    
    tx3.commit();
    
    runWorker();
    Assert.assertEquals(4, exp.intValue());
    exp = null;
    runWorker();
    Assert.assertEquals(5, exp.intValue());
    exp = null;
    runWorker();
    Assert.assertNull(exp);
    
  }
  
  // TODO test that observers trigger on delete
}
