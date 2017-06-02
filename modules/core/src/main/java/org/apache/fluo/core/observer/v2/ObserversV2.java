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

package org.apache.fluo.core.observer.v2;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.observer.Observers;
import org.apache.fluo.core.util.Hex;

class ObserversV2 implements Observers {

  Map<Column, Observer> observers;
  Map<Column, String> aliases;

  public ObserversV2(Environment env, JsonObservers jco, Set<Column> strongColumns,
      Set<Column> weakColumns) {

    ObserverProvider obsProvider =
        ObserverStoreV2.newObserverProvider(jco.getObserverProviderClass());

    ObserverProviderContextImpl ctx = new ObserverProviderContextImpl(env);

    ObserverRegistry or = new ObserverRegistry(strongColumns, weakColumns);
    obsProvider.provide(or, ctx);

    this.observers = or.observers;
    this.aliases = or.aliases;
    this.observers.forEach((k, v) -> aliases.computeIfAbsent(k, col -> Hex.encNonAscii(col, ":")));

    // the following check ensures observers are provided for all previously configured columns
    SetView<Column> diff =
        Sets.difference(observers.keySet(), Sets.union(strongColumns, weakColumns));
    if (diff.size() > 0) {
      throw new FluoException("ObserverProvider " + jco.getObserverProviderClass()
          + " did not provide observers for columns " + diff);
    }
  }

  @Override
  public Observer getObserver(Column col) {
    return observers.get(col);
  }

  @Override
  public void returnObserver(Observer o) {}

  @Override
  public void close() {}

  @Override
  public String getObserverId(Column col) {
    return aliases.get(col);
  }
}
