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

package org.apache.fluo.cluster.runner.it;

import java.util.Collections;
import java.util.List;

import org.apache.fluo.api.config.ObserverConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;
import org.apache.fluo.api.types.TypedObserver;
import org.apache.fluo.api.types.TypedTransaction;
import org.apache.fluo.api.types.TypedTransactionBase;
import org.apache.fluo.cluster.runner.MiniAppRunner;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.integration.ITBaseMini;
import org.junit.Assert;
import org.junit.Test;

public class MiniAppRunnerIT extends ITBaseMini {

  private static TypeLayer tl = new TypeLayer(new StringEncoder());

  public static class TestObserver extends TypedObserver {

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(new Column("ingest", "num"), NotificationType.STRONG);
    }

    @Override
    public void process(TypedTransactionBase tx, Bytes row, Column col) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      Column countColumn = new Column(Bytes.of("count"), col.getQualifier());
      int ingest = tx.get().row(row).col(col).toInteger(0);
      int count = tx.get().row(row).col(countColumn).toInteger(0);
      tx.mutate().row(row).col(countColumn).set(count + ingest);
      tx.mutate().row(row).col(col).delete();
    }
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(TestObserver.class.getName()));
  }

  @Test
  public void testSimple() {
    try (TypedTransaction tx1 = tl.wrap(client.newTransaction())) {
      tx1.mutate().row("row1").fam("ingest").qual("num").set(1);
      tx1.mutate().row("row2").fam("ingest").qual("num").set(1);
      tx1.mutate().row("row3").fam("ingest").qual("num").set(1);
      tx1.commit();
    }

    Environment env = new Environment(config);
    MiniAppRunner runner = new MiniAppRunner();

    Assert.assertEquals(3, runner.countNotifications(env));

    runner.waitUntilFinished(config);

    Assert.assertEquals(0, runner.countNotifications(env));
    Assert.assertEquals(3, runner.scan(config, new String[] {}));
    Assert.assertEquals(1, runner.scan(config, new String[] {"-r", "row1"}));
    Assert.assertEquals(1, runner.scan(config, new String[] {"-r", "row2"}));
    Assert.assertEquals(3, runner.scan(config, new String[] {"-c", "count"}));
    Assert.assertEquals(0, runner.scan(config, new String[] {"-c", "ingest"}));
  }
}
