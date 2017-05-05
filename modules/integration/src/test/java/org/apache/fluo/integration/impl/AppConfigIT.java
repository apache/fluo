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

import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.integration.ITBaseMini;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.fluo.api.observer.Observer.NotificationType.STRONG;

public class AppConfigIT extends ITBaseMini {

  public static final Column DF_COL = new Column("data", "foo");
  public static final Column DB_COL = new Column("data", "bar");

  @Override
  protected void setAppConfig(SimpleConfiguration config) {
    config.setProperty("myapp.sizeLimit", 50000);
  }

  @Override
  protected Class<? extends ObserverProvider> getObserverProviderClass() {
    return TestObserverProvider.class;
  }

  @Test
  public void testBasic() {
    SimpleConfiguration uc = client.getAppConfiguration();
    Assert.assertEquals(50000, uc.getInt("myapp.sizeLimit"));
    uc.setProperty("myapp.sizeLimit", 3);
    uc = client.getAppConfiguration();
    Assert.assertEquals(50000, uc.getInt("myapp.sizeLimit"));

    // update shared config
    SimpleConfiguration appConfig = config.getAppConfiguration();
    appConfig.clear();
    appConfig.setProperty("myapp.sizeLimit", 40000);
    appConfig.setProperty("myapp.timeLimit", 30000);
    try (FluoAdmin admin = FluoFactory.newAdmin(config)) {
      admin.updateSharedConfig();
    }

    // set app config that differs from what was just put in zk
    appConfig.setProperty("myapp.sizeLimit", 6);
    appConfig.setProperty("myapp.timeLimit", 7);

    try (FluoClient client2 = FluoFactory.newClient(config)) {
      uc = client2.getAppConfiguration();
      Assert.assertEquals(40000, uc.getInt("myapp.sizeLimit"));
      Assert.assertEquals(30000, uc.getInt("myapp.timeLimit"));
    }

  }

  private static class TestLoader implements Loader {

    private String row;
    private int data;

    TestLoader(String row, int data) {
      this.row = row;
      this.data = data;
    }

    @Override
    public void load(TransactionBase tx, Context context) throws Exception {
      int limit = context.getAppConfiguration().getInt("myapp.sizeLimit");
      if (data < limit) {
        tx.set(row, DF_COL, Integer.toString(data));
      }
    }
  }

  public static class TestObserverProvider implements ObserverProvider {
    @Override
    public void provide(Registry or, Context ctx) {
      int limit = ctx.getAppConfiguration().getInt("myapp.sizeLimit");

      or.forColumn(DF_COL, STRONG).useStrObserver((tx, row, col) -> {
        int d = Integer.parseInt(tx.gets(row, col));
        if (2 * d < limit) {
          tx.set(row.toString(), DB_COL, Integer.toString(2 * d));
        }
      });
    }

  }

  @Test
  public void testLoaderAndObserver() {

    try (LoaderExecutor le = client.newLoaderExecutor()) {
      le.execute(new TestLoader("r1", 3));
      le.execute(new TestLoader("r2", 30000));
      le.execute(new TestLoader("r3", 60000));
    }

    try (Snapshot snapshot = client.newSnapshot()) {
      Assert.assertEquals("3", snapshot.gets("r1", new Column("data", "foo")));
      Assert.assertEquals("30000", snapshot.gets("r2", new Column("data", "foo")));
      Assert.assertNull(snapshot.gets("r3", new Column("data", "foo")));
    }

    miniFluo.waitForObservers();

    try (Snapshot snapshot = client.newSnapshot()) {
      Assert.assertEquals("6", snapshot.gets("r1", new Column("data", "bar")));
      Assert.assertNull(snapshot.gets("r2", new Column("data", "bar")));
      Assert.assertNull(snapshot.gets("r3", new Column("data", "bar")));
    }
  }
}
