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

import java.util.Collections;
import java.util.List;

import com.google.common.collect.Iterables;
import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.Notification;
import io.fluo.core.util.ByteUtil;
import io.fluo.integration.ITBaseMini;
import io.fluo.integration.TestTransaction;
import io.fluo.integration.impl.WeakNotificationIT.SimpleObserver;
import org.apache.accumulo.core.client.Scanner;
import org.junit.Assert;
import org.junit.Test;

public class NotificationGcIT extends ITBaseMini {

  public static int countRawNotifications(Environment env) throws Exception {
    Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
    scanner.fetchColumnFamily(ByteUtil.toText(ColumnConstants.NOTIFY_CF));
    return Iterables.size(scanner);
  }

  public static int countNotifications(Environment env) throws Exception {
    Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
    Notification.configureScanner(scanner);
    return Iterables.size(scanner);
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(SimpleObserver.class.getName()));
  }

  @Test
  public void testNotificationGC() throws Exception {
    Environment env = new Environment(config);

    TestTransaction tx1 = new TestTransaction(env);
    tx1.mutate().row("r1").fam("stat").qual("count").set(3);
    tx1.done();

    TestTransaction tx2 = new TestTransaction(env);
    tx2.mutate().row("r2").fam("stat").qual("count").set(7);
    tx2.done();

    TestTransaction tx3 = new TestTransaction(env);
    tx3.mutate().row("r1").fam("stats").qual("af89").set(5);
    tx3.mutate().row("r1").fam("stat").qual("check").weaklyNotify();
    tx3.done();

    TestTransaction tx4 = new TestTransaction(env);
    tx4.mutate().row("r2").fam("stats").qual("af99").set(7);
    tx4.mutate().row("r2").fam("stat").qual("check").weaklyNotify();
    tx4.done();

    miniFluo.waitForObservers();

    TestTransaction tx5 = new TestTransaction(env);
    Assert.assertEquals(8, tx5.get().row("r1").fam("stat").qual("count").toInteger(0));
    Assert.assertEquals(14, tx5.get().row("r2").fam("stat").qual("count").toInteger(0));

    Assert.assertEquals(4, countRawNotifications(env));
    Assert.assertEquals(0, countNotifications(env));

    env.getConnector().tableOperations().flush(env.getTable(), null, null, true);

    Assert.assertEquals(0, countRawNotifications(env));
    Assert.assertEquals(0, countNotifications(env));
  }
}
