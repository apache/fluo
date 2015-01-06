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

import java.util.Iterator;
import java.util.Map.Entry;

import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.util.ZookeeperPath;
import io.fluo.accumulo.util.ZookeeperUtil;
import io.fluo.core.BankUtil;
import io.fluo.core.ITBaseImpl;
import io.fluo.core.TestTransaction;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests GarbageCollectionIterator class
 */
public class GarbageCollectionIteratorIT extends ITBaseImpl {

  @Test
  public void testVerifyAfterGC() throws Exception {

    TestTransaction tx1 = new TestTransaction(env);
    BankUtil.setBalance(tx1, "bob", 10);
    BankUtil.setBalance(tx1, "joe", 20);
    BankUtil.setBalance(tx1, "jill", 60);
    tx1.done();

    BankUtil.transfer(env, "joe", "jill", 1);
    BankUtil.transfer(env, "joe", "bob", 1);
    BankUtil.transfer(env, "bob", "joe", 2);
    BankUtil.transfer(env, "jill", "joe", 2);

    TestTransaction tx2 = new TestTransaction(env);

    env.getSharedResources().getTimestampTracker().updateZkNode();
    long oldestTs = ZookeeperUtil.getOldestTimestamp(config.getZookeepers());
    Assert.assertEquals(tx2.getStartTs(), oldestTs);

    // Force a garbage collection
    conn.tableOperations().flush(table, null, null, true);

    verify(oldestTs);

    TestTransaction tx3 = new TestTransaction(env);
    Assert.assertEquals(9, BankUtil.getBalance(tx3, "bob"));
    Assert.assertEquals(22, BankUtil.getBalance(tx3, "joe"));
    Assert.assertEquals(59, BankUtil.getBalance(tx3, "jill"));
    tx3.done();
    tx2.done();
  }
  
  @Test
  public void testGetOldestTimestamp() throws Exception {
    // we are expecting an error in this test
    Level curLevel = Logger.getLogger(ZookeeperUtil.class).getLevel();
    Logger.getLogger(ZookeeperUtil.class).setLevel(Level.FATAL);
    
    // verify that oracle initial current ts
    Assert.assertEquals(0, ZookeeperUtil.getOldestTimestamp(config.getZookeepers()));
    // delete the oracle current timestamp path
    curator.delete().forPath(ZookeeperPath.ORACLE_CUR_TIMESTAMP);
    // verify that oldest possible is returned
    Assert.assertEquals(ZookeeperUtil.OLDEST_POSSIBLE, 
        ZookeeperUtil.getOldestTimestamp(config.getZookeepers()));
    
    // set level back 
    Logger.getLogger(ZookeeperUtil.class).setLevel(curLevel);
  }

  /**
   * Verifies that older versions of data are newer than given timestamp
   *  
   * @param oldestTs
   * @throws TableNotFoundException
   */
  private void verify(long oldestTs) throws TableNotFoundException {
    Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);

    Iterator<Entry<Key, Value>> iter = scanner.iterator();

    Entry<Key,Value> prev = null;
    int numWrites = 0;
    while (iter.hasNext()) {
      Entry<Key,Value> entry = iter.next();

      if ((prev == null) || !prev.getKey().equals(entry.getKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)) {
        numWrites = 0;
      }

      long colType = entry.getKey().getTimestamp() & ColumnConstants.PREFIX_MASK;
      long ts = entry.getKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

      if (colType == ColumnConstants.WRITE_PREFIX) {
        numWrites++;
        if (numWrites > 1) {
          Assert.assertTrue("Extra write had ts "+ts+" < "+oldestTs, ts >= oldestTs);
        }
      }
      prev = entry;
    }
  }
}
