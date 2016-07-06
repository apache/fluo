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

import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.ObserverConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.integration.ITBaseMini;
import org.junit.Assert;
import org.junit.Test;

public class ObserverConfigIT extends ITBaseMini {

  public static class ConfigurableObserver extends AbstractObserver {

    private ObservedColumn observedColumn;
    private Bytes outputCQ;
    private boolean setWeakNotification = false;

    @Override
    public void init(Context context) {
      String ocTokens[] = context.getParameters().get("observedCol").split(":");
      observedColumn =
          new ObservedColumn(new Column(ocTokens[0], ocTokens[1]),
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

  private Map<String, String> newMap(String... args) {
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
    try (Transaction tx1 = client.newTransaction()) {
      tx1.set("r1", new Column("fam1", "col1"), "abcdefg");
      tx1.commit();
    }

    miniFluo.waitForObservers();

    try (Snapshot tx2 = client.newSnapshot()) {
      Assert.assertNull(tx2.gets("r1", new Column("fam1", "col1")));
      Assert.assertNull(tx2.gets("r1", new Column("fam1", "col2")));
      Assert.assertNull(tx2.gets("r1", new Column("fam1", "col3")));
      Assert.assertEquals("abcdefg", tx2.gets("r1", new Column("fam1", "col4")));
    }
  }

}
