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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.integration.ITBaseMini;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.fluo.api.observer.Observer.NotificationType.STRONG;

/**
 * Test an observer notifying the column its observing. This is a useful pattern for exporting data.
 */
public class SelfNotificationIT extends ITBaseMini {

  private static final Column STAT_COUNT_COL = new Column("stat", "count");
  private static final Column EXPORT_CHECK_COL = new Column("export", "check");
  private static final Column EXPORT_COUNT_COL = new Column("export", "count");

  private static List<String> exports = new ArrayList<>();

  public static class ExportingObserver implements Observer {
    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
      String r = row.toString();
      String currentCount = tx.gets(r, STAT_COUNT_COL);
      String exportCount = tx.gets(r, EXPORT_COUNT_COL);

      if (exportCount != null) {
        export(row, exportCount);

        if (currentCount == null || exportCount.equals(currentCount)) {
          tx.delete(row, EXPORT_COUNT_COL);
        } else {
          tx.set(r, EXPORT_COUNT_COL, currentCount);
          tx.set(r, EXPORT_CHECK_COL, "");
        }
      }
    }

    private void export(Bytes row, String exportCount) {
      exports.add(exportCount);
    }
  }

  public static class SelfNtfyObserverProvider implements ObserverProvider {
    @Override
    public void provide(Registry or, Context ctx) {
      or.forColumn(EXPORT_COUNT_COL, STRONG).useObserver(new ExportingObserver());
    }
  }

  @Override
  protected Class<? extends ObserverProvider> getObserverProviderClass() {
    return SelfNtfyObserverProvider.class;
  }

  @Test
  public void test1() throws Exception {

    try (Transaction tx1 = client.newTransaction()) {
      tx1.set("r1", STAT_COUNT_COL, "3");
      tx1.set("r1", EXPORT_CHECK_COL, "");
      tx1.set("r1", EXPORT_COUNT_COL, "3");
      tx1.commit();
    }

    miniFluo.waitForObservers();

    Assert.assertEquals(Collections.singletonList("3"), exports);
    exports.clear();
    miniFluo.waitForObservers();
    Assert.assertEquals(0, exports.size());

    try (Transaction tx2 = client.newTransaction()) {
      Assert.assertNull(tx2.gets("r1", EXPORT_COUNT_COL));

      tx2.set("r1", STAT_COUNT_COL, "5");
      tx2.set("r1", EXPORT_CHECK_COL, "");
      tx2.set("r1", EXPORT_COUNT_COL, "4");

      tx2.commit();
    }

    miniFluo.waitForObservers();
    Assert.assertEquals(Arrays.asList("4", "5"), exports);
    exports.clear();
    miniFluo.waitForObservers();
    Assert.assertEquals(0, exports.size());
  }

  // TODO test self notification w/ weak notifications
}
