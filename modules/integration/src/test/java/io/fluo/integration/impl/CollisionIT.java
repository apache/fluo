/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.integration.impl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.util.ZookeeperUtil;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedLoader;
import io.fluo.api.types.TypedObserver;
import io.fluo.api.types.TypedSnapshot;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.core.impl.FluoConfigurationImpl;
import io.fluo.core.util.UtilWaitThread;
import io.fluo.integration.ITBaseMini;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
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
  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  static class NumLoader extends TypedLoader {

    int num;

    NumLoader(int num) {
      this.num = num;
    }

    @Override
    public void load(TypedTransactionBase tx, Context context) throws Exception {
      tx.mutate().row(num).fam("stat").qual("total").increment(1);
      tx.mutate().row(num).fam("stat").qual("changed").weaklyNotify();
    }
  }

  public static class TotalObserver extends TypedObserver {

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(typeLayer.bc().fam("stat").qual("changed").vis(),
          NotificationType.WEAK);
    }

    @Override
    public void process(TypedTransactionBase tx, Bytes row, Column col) {
      int total = tx.get().row(row).fam("stat").qual("total").toInteger();
      int processed = tx.get().row(row).fam("stat").qual("processed").toInteger(0);

      tx.mutate().row(row).fam("stat").qual("processed").set(total);
      tx.mutate().row("all").fam("stat").qual("total").increment(total - processed);
    }
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Arrays.asList(new ObserverConfiguration(TotalObserver.class.getName()));
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

    try (TypedSnapshot snapshot = typeLayer.wrap(client.newSnapshot())) {

      for (int i = 0; i < expectedCounts.length; i++) {
        Assert.assertEquals(expectedCounts[i], snapshot.get().row(i).fam("stat").qual("total")
            .toInteger(-1));
        Assert.assertEquals(expectedCounts[i], snapshot.get().row(i).fam("stat").qual("processed")
            .toInteger(-1));
      }

      Assert.assertEquals(1000, snapshot.get().row("all").fam("stat").qual("total").toInteger(-1));
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
