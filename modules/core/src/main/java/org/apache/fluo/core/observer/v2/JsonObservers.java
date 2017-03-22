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

import java.util.List;
import java.util.Map;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer.NotificationType;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * this class created for json serialization
 */
class JsonObservers {
  String obsProviderClass;
  List<JsonObservedColumn> observedColumns;

  JsonObservers(String obsProviderClass, Map<Column, NotificationType> columns) {
    this.obsProviderClass = obsProviderClass;
    this.observedColumns =
        columns.entrySet().stream()
            .map(entry -> new JsonObservedColumn(entry.getKey(), entry.getValue()))
            .collect(toList());
  }

  public String getObserverProviderClass() {
    return obsProviderClass;
  }

  public Map<Column, NotificationType> getObservedColumns() {
    return observedColumns.stream().collect(
        toMap(JsonObservedColumn::getColumn, JsonObservedColumn::getNotificationType));
  }

  @Override
  public String toString() {
    return obsProviderClass + " " + getObservedColumns();
  }
}
