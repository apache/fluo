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

import java.util.Map.Entry;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.integration.ITBaseMini;
import org.apache.fluo.integration.TestTransaction;
import org.apache.fluo.integration.impl.WeakNotificationIT.WeakNotificationITObserverProvider;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationGcIT extends ITBaseMini {

  private static final Logger log = LoggerFactory.getLogger(NotificationGcIT.class);

  private static void assertRawNotifications(int expected, Environment env) throws Exception {
    Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
    scanner.fetchColumnFamily(ByteUtil.toText(ColumnConstants.NOTIFY_CF));
    int size = Iterables.size(scanner);
    if (size != expected) {
      for (Entry<Key, Value> entry : scanner) {
        log.error(entry.toString());
      }
    }
    Assert.assertEquals(expected, size);
  }

  private static int countNotifications(Environment env) throws Exception {
    Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
    Notification.configureScanner(scanner);
    return Iterables.size(scanner);
  }

  @Override
  protected Class<? extends ObserverProvider> getObserverProviderClass() {
    return WeakNotificationITObserverProvider.class;
  }

  @Test
  public void testNotificationGC() throws Exception {

    final Column statCount = new Column("stat", "count");
    final Column statCheck = new Column("stat", "check");

    Environment env = new Environment(config);

    TestTransaction tx1 = new TestTransaction(env);
    tx1.set("r1", statCount, 3 + "");
    tx1.done();

    TestTransaction tx2 = new TestTransaction(env);
    tx2.set("r2", statCount, 7 + "");
    tx2.done();

    TestTransaction tx3 = new TestTransaction(env);
    tx3.set("r1", new Column("stats", "af89"), 5 + "");
    tx3.setWeakNotification("r1", statCheck);
    tx3.done();

    TestTransaction tx4 = new TestTransaction(env);
    tx4.set("r2", new Column("stats", "af99"), 7 + "");
    tx4.setWeakNotification("r2", statCheck);
    tx4.done();

    miniFluo.waitForObservers();

    TestTransaction tx5 = new TestTransaction(env);
    Assert.assertEquals("8", tx5.gets("r1", statCount));
    Assert.assertEquals("14", tx5.gets("r2", statCount));

    assertRawNotifications(4, env);
    Assert.assertEquals(0, countNotifications(env));

    env.getConnector().tableOperations().flush(env.getTable(), null, null, true);

    assertRawNotifications(0, env);
    Assert.assertEquals(0, countNotifications(env));
  }
}
