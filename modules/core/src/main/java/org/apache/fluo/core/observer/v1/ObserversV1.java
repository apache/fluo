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

package org.apache.fluo.core.observer.v1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Iterables;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.observer.Observers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
class ObserversV1 implements Observers {

  private static final Logger log = LoggerFactory.getLogger(ObserversV1.class);

  private Environment env;
  Map<Column, List<Observer>> observers = new HashMap<>();
  Map<Column, ObserverSpecification> strongObservers;
  Map<Column, ObserverSpecification> weakObservers;
  Map<Column, String> aliases;

  private List<Observer> getObserverList(Column col) {
    List<Observer> observerList;
    synchronized (observers) {
      observerList = observers.get(col);
      if (observerList == null) {
        observerList = new ArrayList<>();
        observers.put(col, observerList);
      }
    }
    return observerList;
  }

  public ObserversV1(Environment env, Map<Column, ObserverSpecification> strongObservers,
      Map<Column, ObserverSpecification> weakObservers) {
    this.env = env;
    this.strongObservers = strongObservers;
    this.weakObservers = weakObservers;
    this.aliases = new HashMap<>();

    for (Entry<Column, ObserverSpecification> e : Iterables.concat(strongObservers.entrySet(),
        weakObservers.entrySet())) {
      ObserverSpecification observerConfig = e.getValue();
      try {
        String alias =
            Class.forName(observerConfig.getClassName()).asSubclass(Observer.class).getSimpleName();
        aliases.put(e.getKey(), alias);
      } catch (ClassNotFoundException e1) {
        throw new RuntimeException(e1);
      }
    }
  }

  public Observer getObserver(Column col) {

    List<Observer> observerList;
    observerList = getObserverList(col);

    synchronized (observerList) {
      if (observerList.size() > 0) {
        return observerList.remove(observerList.size() - 1);
      }
    }

    Observer observer = null;

    ObserverSpecification observerConfig = strongObservers.get(col);
    if (observerConfig == null) {
      observerConfig = weakObservers.get(col);
    }

    if (observerConfig != null) {
      try {
        observer =
            Class.forName(observerConfig.getClassName()).asSubclass(Observer.class).newInstance();
        observer.init(new ObserverContext(env, observerConfig.getConfiguration()));
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      if (!observer.getObservedColumn().getColumn().equals(col)) {
        throw new IllegalStateException("Mismatch between configured column and class column "
            + observerConfig.getClassName() + " " + col + " "
            + observer.getObservedColumn().getColumn());
      }
    }

    return observer;
  }

  public void returnObserver(Observer observer) {
    List<Observer> olist = getObserverList(observer.getObservedColumn().getColumn());
    synchronized (olist) {
      olist.add(observer);
    }
  }

  @Override
  public void close() {
    if (observers == null) {
      return;
    }

    synchronized (observers) {
      for (List<Observer> olist : observers.values()) {
        synchronized (olist) {
          for (Observer observer : olist) {
            try {
              observer.close();
            } catch (Exception e) {
              log.error("Failed to close observer", e);
            }
          }
          olist.clear();
        }
      }
    }

    observers = null;
  }

  @Override
  public String getObserverId(Column col) {
    return aliases.get(col);
  }
}
