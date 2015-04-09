/*
 * Copyright 2015 Fluo authors (see AUTHORS)
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
package io.fluo.cluster.runner;

import java.util.Collections;
import java.util.List;

import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedObserver;
import io.fluo.api.types.TypedTransaction;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.core.ITBaseMini;
import io.fluo.core.impl.Environment;
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
      tx.mutate().row(row).col(countColumn).set(count+ingest);
      tx.mutate().row(row).col(col).delete();
    }
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(TestObserver.class.getName()));
  }

  @Test
  public void testSimple() {
    try(TypedTransaction tx1 = tl.wrap(client.newTransaction())){
      tx1.mutate().row("row1").fam("ingest").qual("num").set(1);
      tx1.mutate().row("row2").fam("ingest").qual("num").set(1);
      tx1.mutate().row("row3").fam("ingest").qual("num").set(1);
      tx1.commit();
    }

    Environment env = new Environment(config);
    MiniAppRunner runner = new MiniAppRunner(config);

    Assert.assertEquals(3, runner.countNotifications(env));

    runner.waitUntilFinished();

    Assert.assertEquals(0, runner.countNotifications(env));
    Assert.assertEquals(3, runner.scan(new String[] {}));
    Assert.assertEquals(1, runner.scan(new String[] {"-r", "row1"}));
    Assert.assertEquals(1, runner.scan(new String[] {"-r", "row2"}));
    Assert.assertEquals(3, runner.scan(new String[] {"-c", "count"}));
    Assert.assertEquals(0, runner.scan(new String[] {"-c", "ingest"}));
  }
}
