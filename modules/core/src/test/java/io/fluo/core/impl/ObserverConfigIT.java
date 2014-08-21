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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.fluo.api.client.Transaction;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.observer.AbstractObserver;
import io.fluo.api.observer.Observer.NotificationType;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.core.TestBaseImpl;
import io.fluo.core.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

public class ObserverConfigIT extends TestBaseImpl {

  private static TypeLayer tl = new TypeLayer(new StringEncoder());

  public static class ConfigurableObserver extends AbstractObserver {

    private ObservedColumn observedColumn;
    private Bytes outputCQ;
    private boolean setWeakNotification = false;

    @Override
    public void init(Map<String,String> config) {
      String ocTokens[] = config.get("observedCol").split(":");
      observedColumn = new ObservedColumn(tl.newColumn(ocTokens[0], ocTokens[1]), NotificationType.valueOf(ocTokens[2]));
      outputCQ = Bytes.wrap(config.get("outputCQ"));
      String swn = config.get("setWeakNotification");
      if (swn != null && swn.equals("true"))
        setWeakNotification = true;
    }

    @Override
    public void process(Transaction tx, Bytes row, Column col) throws Exception {

      Bytes in = tx.get(row, col);
      tx.delete(row, col);

      Column outCol = new Column(col.getFamily(), outputCQ);

      tx.set(row, outCol, in);

      if (setWeakNotification)
        tx.setWeakNotification(row, outCol);
    }

    @Override
    public ObservedColumn getObservedColumn() {
      return observedColumn;
    }
  }

  Map<String,String> newMap(String... args) {
    HashMap<String,String> ret = new HashMap<String,String>();
    for (int i = 0; i < args.length; i += 2)
      ret.put(args[i], args[i + 1]);
    return ret;
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    List<ObserverConfiguration> observers = new ArrayList<ObserverConfiguration>();

    observers.add(new ObserverConfiguration(ConfigurableObserver.class.getName()).setParameters(newMap("observedCol", "fam1:col1:" + NotificationType.STRONG,
        "outputCQ", "col2")));

    observers.add(new ObserverConfiguration(ConfigurableObserver.class.getName()).setParameters(newMap("observedCol", "fam1:col2:" + NotificationType.STRONG,
        "outputCQ", "col3", "setWeakNotification", "true")));

    observers.add(new ObserverConfiguration(ConfigurableObserver.class.getName()).setParameters(newMap("observedCol", "fam1:col3:" + NotificationType.WEAK,
        "outputCQ", "col4")));

    return observers;
  }

  @Test
  public void testObserverConfig() throws Exception {

    TestTransaction tx1 = new TestTransaction(env);
    tx1.mutate().row("r1").fam("fam1").qual("col1").set("abcdefg");
    tx1.commit();

    runWorker();

    TestTransaction tx2 = new TestTransaction(env);
    Assert.assertNull(tx2.get().row("r1").fam("fam1").qual("col1").toString());
    Assert.assertNull(tx2.get().row("r1").fam("fam1").qual("col2").toString());
    Assert.assertNull(tx2.get().row("r1").fam("fam1").qual("col3").toString());
    Assert.assertEquals("abcdefg", tx2.get().row("r1").fam("fam1").qual("col4").toString());
  }

}
