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

import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.core.impl.Environment;

public class ObserverContext implements Observer.Context {

  private final SimpleConfiguration observerConfig;
  private final SimpleConfiguration appConfig;
  private final Environment env;

  public ObserverContext(SimpleConfiguration appConfig, SimpleConfiguration observerConfig) {
    this.appConfig = appConfig;
    this.observerConfig = observerConfig;
    this.env = null;
  }

  public ObserverContext(Environment env, SimpleConfiguration observerConfig) {
    this.env = env;
    this.appConfig = null;
    this.observerConfig = observerConfig;
  }

  @Override
  public SimpleConfiguration getAppConfiguration() {
    if (env == null) {
      return appConfig;
    }
    return env.getAppConfiguration();
  }

  @Override
  public SimpleConfiguration getObserverConfiguration() {
    return observerConfig;
  }

}
