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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.fluo.api.client.Transaction;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedTransaction;
import io.fluo.core.TestBaseImpl;
import io.fluo.core.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test an observer notifying the column its observing.  This is a useful pattern for exporting data.
 */
public class SelfNotificationIT extends TestBaseImpl {
  
  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());
  
  static final Column STAT_COUNT_COL = typeLayer.bc().fam("stat").qual("count").vis();
  static final Column EXPORT_CHECK_COL = typeLayer.bc().fam("export").qual("check").vis();
  static final Column EXPORT_COUNT_COL = typeLayer.bc().fam("export").qual("count").vis();

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(ExportingObserver.class.getName()));
  }

  static List<Integer> exports = new ArrayList<Integer>();
  
  public static class ExportingObserver extends AbstractObserver {
    
    @Override
    public void process(Transaction tx, Bytes row, Column col) throws Exception {

      TypedTransaction ttx = typeLayer.wrap(tx);
       
      Integer currentCount = ttx.get().row(row).col(STAT_COUNT_COL).toInteger();
      Integer exportCount = ttx.get().row(row).col(EXPORT_COUNT_COL).toInteger();
      
      if(exportCount != null){
        export(row, exportCount);
        
        if(currentCount == null || exportCount.equals(currentCount)){
          ttx.mutate().row(row).col(EXPORT_COUNT_COL).delete();
        }else{
          ttx.mutate().row(row).col(EXPORT_COUNT_COL).set(currentCount);
          ttx.mutate().row(row).col(EXPORT_CHECK_COL).set();
        }
        
      }
    }

    private void export(Bytes row, Integer exportCount) {
      exports.add(exportCount);
    }

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(EXPORT_COUNT_COL, NotificationType.STRONG);
    }
  }
  
  @Test
  public void test1() throws Exception {
    
    TestTransaction tx1 = new TestTransaction(env);

    tx1.mutate().row("r1").col(STAT_COUNT_COL).set(3);
    tx1.mutate().row("r1").col(EXPORT_CHECK_COL).set();
    tx1.mutate().row("r1").col(EXPORT_COUNT_COL).set(3);
    
    tx1.commit();
    
    runWorker();
    Assert.assertEquals(Collections.singletonList(3), exports);
    exports.clear();
    runWorker();
    Assert.assertEquals(0, exports.size());
   
    TestTransaction tx2 = new TestTransaction(env);

    Assert.assertNull(tx2.get().row("r1").col(EXPORT_COUNT_COL).toInteger());
    
    tx2.mutate().row("r1").col(STAT_COUNT_COL).set(4);
    tx2.mutate().row("r1").col(EXPORT_CHECK_COL).set();
    tx2.mutate().row("r1").col(EXPORT_COUNT_COL).set(4);
    
    tx2.commit();
    
    TestTransaction tx3 = new TestTransaction(env);

    tx3.mutate().row("r1").col(STAT_COUNT_COL).set(5);
    tx3.mutate().row("r1").col(EXPORT_CHECK_COL).set();
    
    tx3.commit();
    
    runWorker();
    Assert.assertEquals(Arrays.asList(4, 5), exports);
    exports.clear();
    runWorker();
    Assert.assertEquals(0, exports.size());
    
  }

  // TODO test self notification w/ weak notifications
}
