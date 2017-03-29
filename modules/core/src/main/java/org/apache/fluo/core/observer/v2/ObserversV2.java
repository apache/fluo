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

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.api.observer.ObserverProvider.Registry;
import org.apache.fluo.api.observer.StringObserver;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.observer.Observers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ObserversV2 implements Observers {

  private static final Logger log = LoggerFactory.getLogger(ObserversV2.class);

  Map<Column, Observer> observers;

  public ObserversV2(Environment env, JsonObservers jco, Set<Column> strongColumns,
      Set<Column> weakColumns) {
    observers = new HashMap<>();

    ObserverProvider obsProvider =
        ObserverStoreV2.newObserverProvider(jco.getObserverProviderClass());

    ObserverProviderContextImpl ctx = new ObserverProviderContextImpl(env);

    Registry or = new Registry() {

      @Override
      public void register(Column col, NotificationType nt, Observer obs) {
        try {
          Method closeMethod = obs.getClass().getMethod("close");
          if (!closeMethod.getDeclaringClass().equals(Observer.class)) {
            log.warn(
                "Observer {} implements close().  Close is not called on Observers created using ObserverProvider."
                    + " Close is only called on Observers configured the old way.", obs.getClass()
                    .getName());
          }
        } catch (NoSuchMethodException | SecurityException e) {
          throw new RuntimeException("Failed to check if close() is implemented", e);
        }

        if (nt == NotificationType.STRONG && !strongColumns.contains(col)) {
          throw new IllegalArgumentException("Column " + col
              + " not previously configured for strong notifications");
        }

        if (nt == NotificationType.WEAK && !weakColumns.contains(col)) {
          throw new IllegalArgumentException("Column " + col
              + " not previously configured for weak notifications");
        }

        if (observers.containsKey(col)) {
          throw new IllegalArgumentException("Duplicate observed column " + col);
        }

        observers.put(col, obs);
      }

      @Override
      public void registers(Column col, NotificationType nt, StringObserver obs) {
        register(col, nt, obs);
      }
    };

    obsProvider.provide(or, ctx);

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
}
