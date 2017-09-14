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

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.api.observer.StringObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObserverRegistry implements ObserverProvider.Registry {

  private static final Logger log = LoggerFactory.getLogger(ObserverRegistry.class);

  Map<Column, Observer> observers;
  Map<Column, String> aliases;
  private Set<Column> strongColumns;
  private Set<Column> weakColumns;

  private class FluentRegistration implements ObserverProvider.Registry.IdentityOption,
      ObserverProvider.Registry.ObserverArgument {

    private Column col;
    private NotificationType ntfyType;
    private String alias;

    FluentRegistration(Column col, NotificationType ntfyType) {
      this.col = col;
      this.ntfyType = ntfyType;
    }

    @Override
    public void useObserver(Observer observer) {
      register(col, ntfyType, alias, observer);
    }

    @Override
    public void useStrObserver(StringObserver observer) {
      register(col, ntfyType, alias, observer);
    }

    @Override
    public ObserverArgument withId(String alias) {
      this.alias = alias;
      return this;
    }
  }

  ObserverRegistry(Set<Column> strongColumns, Set<Column> weakColumns) {
    this.observers = new HashMap<>();
    this.aliases = new HashMap<>();
    this.strongColumns = strongColumns;
    this.weakColumns = weakColumns;
  }

  @Override
  public IdentityOption forColumn(Column observedColumn, NotificationType ntfyType) {
    return new FluentRegistration(observedColumn, ntfyType);
  }

  private void register(Column col, NotificationType nt, String alias, Observer obs) {
    try {
      Method closeMethod = obs.getClass().getMethod("close");
      if (!closeMethod.getDeclaringClass().equals(Observer.class)) {
        log.warn(
            "Observer {} implements close().  Close is not called on Observers registered using ObserverProvider."
                + " Close is only called on Observers configured the old way.",
            obs.getClass().getName());
      }
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("Failed to check if close() is implemented", e);
    }

    if (nt == NotificationType.STRONG && !strongColumns.contains(col)) {
      throw new IllegalArgumentException(
          "Column " + col + " not previously configured for strong notifications");
    }

    if (nt == NotificationType.WEAK && !weakColumns.contains(col)) {
      throw new IllegalArgumentException(
          "Column " + col + " not previously configured for weak notifications");
    }

    if (observers.containsKey(col)) {
      throw new IllegalArgumentException("Duplicate observed column " + col);
    }

    observers.put(col, obs);
    aliases.put(col, alias);
  }

}
