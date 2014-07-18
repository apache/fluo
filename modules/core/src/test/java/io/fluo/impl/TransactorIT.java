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
package io.fluo.impl;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests transactor class
 */
public class TransactorIT extends Base {
  
  public static Long id1 = new Long(1);
  public static Long id2 = new Long(2);
  
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testTransactorAndCache() throws Exception {
    
    TransactorCache cache = new TransactorCache(config);
    Assert.assertFalse(cache.checkExists(id1));
    Assert.assertFalse(cache.checkExists(id2));
        
    // create 2 transactors
    TransactorID t1 = new TransactorID(config);    
    TransactorID t2 = new TransactorID(config);

    // verify that they were created correctly
    Assert.assertEquals(2, getNumOpen());
    Assert.assertEquals(id1, t1.getLongID());
    Assert.assertEquals(id2, t2.getLongID());
    Assert.assertTrue(checkExists(t1));
    Assert.assertTrue(checkExists(t2));
    Assert.assertArrayEquals("1".getBytes(), curator.getData().forPath(t1.getNodePath()));
    Assert.assertArrayEquals("2".getBytes(), curator.getData().forPath(t2.getNodePath()));
    
    // verify the cache
    Assert.assertTrue(cache.checkExists(id1));
    Assert.assertTrue(cache.checkExists(id2));
    
    // close transactor 1
    t1.close();
    
    // verify that t1 was closed
    Assert.assertEquals(1, getNumOpen());
    Assert.assertFalse(checkExists(t1));
    Assert.assertTrue(checkExists(t2));
    Assert.assertFalse(cache.checkExists(id1));
    Assert.assertTrue(cache.checkExists(id2));
    
    // close transactor 2
    t2.close();
    
    // verify that t2 closed
    Assert.assertEquals(0, getNumOpen());
    Assert.assertFalse(checkExists(t2));
    
    // verify the cache
    Assert.assertFalse(cache.checkExists(id1));
    Assert.assertFalse(cache.checkExists(id2));
    
    cache.close();
  }
  
  @Test
  public void testFailures() throws Exception {
    
    TransactorID t1 = new TransactorID(config);  
    
    Assert.assertEquals(1, getNumOpen());
    Assert.assertEquals(id1, t1.getLongID());
    Assert.assertTrue(checkExists(t1));
   
    // Test that node will be recreated if removed
    curator.delete().forPath(t1.getNodePath());
    
    Assert.assertEquals(id1, t1.getLongID());
    Assert.assertEquals(1, getNumOpen());
    Assert.assertTrue(checkExists(t1));
    
    t1.close();
    
    Assert.assertEquals(0, getNumOpen());
    Assert.assertFalse(checkExists(t1));
    
    // Test for exception to be called
    exception.expect(IllegalStateException.class);
    t1.getLongID();
  }
  
  private boolean checkExists(TransactorID t) throws Exception {
    return curator.checkExists().forPath(t.getNodePath()) != null;
  }
  
  private int getNumOpen() throws Exception {
    return curator.getChildren().forPath(config.getZookeeperRoot()+Constants.Zookeeper.TRANSACTOR_NODES).size();
  }
}
