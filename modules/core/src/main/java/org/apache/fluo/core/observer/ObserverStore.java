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

package org.apache.fluo.core.observer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.api.config.FluoConfiguration;

/*
 * This interface enables abstracting the new and old way on configuring observers.
 */
public interface ObserverStore {
  boolean handles(FluoConfiguration config);

  void clear(CuratorFramework curator) throws Exception;

  void update(CuratorFramework curator, FluoConfiguration config) throws Exception;

  RegisteredObservers load(CuratorFramework curator) throws Exception;
}
