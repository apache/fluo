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

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.observer.Observer.NotificationType;

/**
 * this class created for json serialization
 */
class JsonObservedColumn {
  private byte[] fam;
  private byte[] qual;
  private byte[] vis;
  private String notificationType;

  JsonObservedColumn(Column col, NotificationType nt) {
    this.fam = col.getFamily().toArray();
    this.qual = col.getQualifier().toArray();
    this.vis = col.getVisibility().toArray();
    this.notificationType = nt.name();
  }

  public Column getColumn() {
    return new Column(Bytes.of(fam), Bytes.of(qual), Bytes.of(vis));
  }

  public NotificationType getNotificationType() {
    return NotificationType.valueOf(notificationType);
  }
}
