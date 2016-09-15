/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.integration.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.ZookeeperUtil;
import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.fluo.integration.ITBaseMini;
import org.apache.fluo.integration.TestUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Run end to end test with lots of collisions and verify the following :
 *
 * <p>
 * <ul>
 * <li/>LoaderExecutor works correctly w/ lots of collisions
 * <li/>Observers work correctly w/ lots of collisions
 * <li/>Fluo GC works correctly after lots of collisions
 * </ul>
 *
 */
public class CollisionIT extends ITBaseMini {

  private static final Column STAT_TOTAL = new Column("stat", "total");
  private static final Column STAT_CHANGED = new Column("stat", "changed");
  private static final Column STAT_PROCESSED = new Column("stat", "processed");

  private static class NumLoader implements Loader {

    int num;

    NumLoader(int num) {
      this.num = num;
    }

    @Override
    public void load(TransactionBase tx, Context context) throws Exception {
      TestUtil.increment(tx, num + "", STAT_TOTAL, 1);
      tx.setWeakNotification(num + "", STAT_CHANGED);
    }
  }

  public static class TotalObserver extends AbstractObserver {

    @Override
    public Observer.ObservedColumn getObservedColumn() {
      return new Observer.ObservedColumn(STAT_CHANGED, NotificationType.WEAK);
    }

    @Override
    public void process(TransactionBase tx, Bytes rowBytes, Column col) throws Exception {
      String row = rowBytes.toString();
      int total = Integer.parseInt(tx.gets(row, STAT_TOTAL));
      int processed = TestUtil.getOrDefault(tx, row, STAT_PROCESSED, 0);

      tx.set(row, STAT_PROCESSED, total + "");
      TestUtil.increment(tx, "all", STAT_TOTAL, total - processed);
    }
  }

  @Override
  protected List<ObserverSpecification> getObservers() {
    return Collections.singletonList(new ObserverSpecification(TotalObserver.class.getName()));
  }

  @Override
  protected void setConfig(FluoConfiguration config) {
    config.setLoaderQueueSize(20);
    config.setLoaderThreads(20);
    config.setWorkerThreads(20);

    // make updates in ZK related to Fluo GC more frequent
    config.setProperty(FluoConfigurationImpl.ZK_UPDATE_PERIOD_PROP, "100");
  }

  @Test
  public void testLotsOfCollisions() throws Exception {

    Random rand = new Random(45734985);

    int[] nums = new int[1000];
    int[] expectedCounts = new int[5];

    for (int i = 0; i < nums.length; i++) {
      nums[i] = rand.nextInt(expectedCounts.length);
      expectedCounts[nums[i]]++;
    }

    try (LoaderExecutor loader = client.newLoaderExecutor()) {
      for (int num : nums) {
        loader.execute(new NumLoader(num));
      }
    }

    miniFluo.waitForObservers();

    try (Snapshot snapshot = client.newSnapshot()) {

      for (int i = 0; i < expectedCounts.length; i++) {
        String total = snapshot.gets(i + "", STAT_TOTAL);
        Assert.assertNotNull(total);
        Assert.assertEquals(expectedCounts[i], Integer.parseInt(total));
        String processed = snapshot.gets(i + "", STAT_PROCESSED);
        Assert.assertNotNull(processed);
        Assert.assertEquals(expectedCounts[i], Integer.parseInt(processed));
      }

      String allTotal = snapshot.gets("all", STAT_TOTAL);
      Assert.assertNotNull(allTotal);
      Assert.assertEquals(1000, Integer.parseInt(allTotal));
    }

    long oldestTS = ZookeeperUtil.getGcTimestamp(config.getAppZookeepers());
    while (true) {
      UtilWaitThread.sleep(300);
      long tmp = ZookeeperUtil.getGcTimestamp(config.getAppZookeepers());
      if (oldestTS == tmp) {
        break;
      }
      oldestTS = tmp;
    }

    conn.tableOperations().compact(getCurTableName(), null, null, true, true);

    Scanner scanner = conn.createScanner(getCurTableName(), Authorizations.EMPTY);

    HashSet<String> rowCols = new HashSet<>();

    for (Entry<Key, Value> entry : scanner) {
      Key k = entry.getKey();
      String rowCol =
          k.getRow() + ":" + k.getColumnFamily() + ":" + k.getColumnQualifier() + ":"
              + String.format("%x", k.getTimestamp() & ColumnConstants.PREFIX_MASK);
      Assert.assertFalse("Duplicate row col " + rowCol, rowCols.contains(rowCol));
      rowCols.add(rowCol);
    }
  }
}
