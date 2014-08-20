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

import io.fluo.core.TestBaseImpl;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** 
 * Tests transactor class
 */
public class TransactorIT extends TestBaseImpl {
  
  public static Long id1 = new Long(1);
  public static Long id2 = new Long(2);
  public static long NUM_OPEN_TIMEOUT_MS = 1000;
  
  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void testTransactorAndCache() throws Exception {
    
    TransactorCache cache = new TransactorCache(env);
    Assert.assertFalse(cache.checkExists(id1));
    Assert.assertFalse(cache.checkExists(id2));
        
    // create 2 transactors
    TransactorID t1 = new TransactorID(env);    
    TransactorID t2 = new TransactorID(env);

    // verify that they were created correctly
    assertNumOpen(2);
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
    assertNumOpen(1);
    Assert.assertFalse(checkExists(t1));
    Assert.assertTrue(checkExists(t2));
    Assert.assertFalse(cache.checkExists(id1));
    Assert.assertTrue(cache.checkExists(id2));
    
    // close transactor 2
    t2.close();
    
    // verify that t2 closed
    assertNumOpen(0);
    Assert.assertFalse(checkExists(t2));
    
    // verify the cache
    Assert.assertFalse(cache.checkExists(id1));
    Assert.assertFalse(cache.checkExists(id2));
    
    cache.close();
  }
  
  @Test
  public void testFailures() throws Exception {
    
    TransactorID t1 = new TransactorID(env);  
    
    assertNumOpen(1);
    Assert.assertEquals(id1, t1.getLongID());
    Assert.assertTrue(checkExists(t1));
   
    // Test that node will be recreated if removed
    curator.delete().forPath(t1.getNodePath());
    
    Assert.assertEquals(id1, t1.getLongID());
    assertNumOpen(1);
    Assert.assertTrue(checkExists(t1));
    
    t1.close();
    
    assertNumOpen(0);
    Assert.assertFalse(checkExists(t1));
    
    // Test for exception to be called
    exception.expect(IllegalStateException.class);
    t1.getLongID();
  }
  
  @Test(timeout = 30000)
  public void testTimeout() throws Exception {
    TransactorCache cache = new TransactorCache(env);

    cache.addTimedoutTransactor(id1, 4, System.currentTimeMillis() - 3);

    Assert.assertTrue(cache.checkTimedout(id1, 3));
    Assert.assertTrue(cache.checkTimedout(id1, 4));
    Assert.assertFalse(cache.checkTimedout(id1, 5));
    Assert.assertFalse(cache.checkTimedout(id1, 6));

    cache.addTimedoutTransactor(id1, 7, System.currentTimeMillis() - 3);
    cache.addTimedoutTransactor(id2, 4, System.currentTimeMillis() - 3);

    Assert.assertTrue(cache.checkTimedout(id1, 4));
    Assert.assertTrue(cache.checkTimedout(id1, 5));
    Assert.assertTrue(cache.checkTimedout(id1, 6));
    Assert.assertTrue(cache.checkTimedout(id1, 7));
    Assert.assertFalse(cache.checkTimedout(id1, 8));
    Assert.assertFalse(cache.checkTimedout(id1, 9));

    Assert.assertTrue(cache.checkTimedout(id2, 3));
    Assert.assertTrue(cache.checkTimedout(id2, 4));
    Assert.assertFalse(cache.checkTimedout(id2, 5));
    Assert.assertFalse(cache.checkTimedout(id2, 6));

    // esnure setting a lower lockTs than previously set has no effect
    cache.addTimedoutTransactor(id1, 3, System.currentTimeMillis() - 3);

    Assert.assertTrue(cache.checkTimedout(id1, 7));
    Assert.assertFalse(cache.checkTimedout(id1, 8));

    cache.close();
  }

  private boolean checkExists(TransactorID t) throws Exception {
    return curator.checkExists().forPath(t.getNodePath()) != null;
  }
  
  private int getNumOpen() throws Exception {
    return curator.getChildren().forPath(env.getZookeeperRoot()+ZookeeperConstants.TRANSACTOR_NODES).size();
  }
  
  private void assertNumOpen(int expected) throws Exception {
    long startTime = System.currentTimeMillis();
    while (getNumOpen() != expected) {
      Thread.sleep(50);
      if ((System.currentTimeMillis() - startTime) > NUM_OPEN_TIMEOUT_MS) {
        Assert.fail("Timed out waiting for correct transactor number in Zookeeper");
      }
    }
  }
}
