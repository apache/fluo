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

package org.apache.fluo.core.worker;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.worker.finder.hash.PartitionNotificationFinder;

public class NotificationFinderFactory {
  public static NotificationFinder newNotificationFinder(FluoConfiguration conf) {
    String clazz = conf.getString(FluoConfigurationImpl.WORKER_FINDER_PROP,
        PartitionNotificationFinder.class.getName());
    try {
      return Class.forName(clazz).asSubclass(NotificationFinder.class).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

  }
}
