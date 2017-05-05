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

package org.apache.fluo.api.observer;

import java.util.function.BiConsumer;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.ObserverProvider.Registry.ObserverArgument;

// Intentionally package private
class ColumnProviderRegistry implements ObserverProvider.Registry.ObserverArgument,
    ObserverProvider.Registry.IdentityOption {

  private BiConsumer<Column, NotificationType> colRegistry;
  private NotificationType nt;
  private Column col;

  ColumnProviderRegistry(Column col, NotificationType nt,
      BiConsumer<Column, NotificationType> colRegistry) {
    this.col = col;
    this.nt = nt;
    this.colRegistry = colRegistry;
  }

  @Override
  public ObserverArgument withId(String alias) {
    return this;
  }

  @Override
  public void useObserver(Observer observer) {
    colRegistry.accept(col, nt);
  }

  @Override
  public void useStrObserver(StringObserver observer) {
    useObserver(observer);
  }

}
