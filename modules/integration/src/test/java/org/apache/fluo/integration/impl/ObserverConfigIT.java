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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.ObserverConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;
import org.apache.fluo.api.types.TypedSnapshot;
import org.apache.fluo.api.types.TypedTransaction;
import org.apache.fluo.integration.ITBaseMini;
import org.junit.Assert;
import org.junit.Test;

public class ObserverConfigIT extends ITBaseMini {

  private static TypeLayer tl = new TypeLayer(new StringEncoder());

  public static class ConfigurableObserver extends AbstractObserver {

    private ObservedColumn observedColumn;
    private Bytes outputCQ;
    private boolean setWeakNotification = false;

    @Override
    public void init(Context context) {
      String ocTokens[] = context.getParameters().get("observedCol").split(":");
      observedColumn =
          new ObservedColumn(tl.bc().fam(ocTokens[0]).qual(ocTokens[1]).vis(),
              NotificationType.valueOf(ocTokens[2]));
      outputCQ = Bytes.of(context.getParameters().get("outputCQ"));
      String swn = context.getParameters().get("setWeakNotification");
      if (swn != null && swn.equals("true")) {
        setWeakNotification = true;
      }
    }

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) throws Exception {

      Bytes in = tx.get(row, col);
      tx.delete(row, col);

      Column outCol = new Column(col.getFamily(), outputCQ);

      tx.set(row, outCol, in);

      if (setWeakNotification) {
        tx.setWeakNotification(row, outCol);
      }
    }

    @Override
    public ObservedColumn getObservedColumn() {
      return observedColumn;
    }
  }

  Map<String, String> newMap(String... args) {
    HashMap<String, String> ret = new HashMap<>();
    for (int i = 0; i < args.length; i += 2) {
      ret.put(args[i], args[i + 1]);
    }
    return ret;
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    List<ObserverConfiguration> observers = new ArrayList<>();

    observers.add(new ObserverConfiguration(ConfigurableObserver.class.getName())
        .setParameters(newMap("observedCol", "fam1:col1:" + NotificationType.STRONG, "outputCQ",
            "col2")));

    observers.add(new ObserverConfiguration(ConfigurableObserver.class.getName())
        .setParameters(newMap("observedCol", "fam1:col2:" + NotificationType.STRONG, "outputCQ",
            "col3", "setWeakNotification", "true")));

    observers.add(new ObserverConfiguration(ConfigurableObserver.class.getName())
        .setParameters(newMap("observedCol", "fam1:col3:" + NotificationType.WEAK, "outputCQ",
            "col4")));

    return observers;
  }

  @Test
  public void testObserverConfig() throws Exception {
    try (TypedTransaction tx1 = tl.wrap(client.newTransaction())) {
      tx1.mutate().row("r1").fam("fam1").qual("col1").set("abcdefg");
      tx1.commit();
    }

    miniFluo.waitForObservers();

    try (TypedSnapshot tx2 = tl.wrap(client.newSnapshot())) {
      Assert.assertNull(tx2.get().row("r1").fam("fam1").qual("col1").toString());
      Assert.assertNull(tx2.get().row("r1").fam("fam1").qual("col2").toString());
      Assert.assertNull(tx2.get().row("r1").fam("fam1").qual("col3").toString());
      Assert.assertEquals("abcdefg", tx2.get().row("r1").fam("fam1").qual("col4").toString());
    }
  }

}
