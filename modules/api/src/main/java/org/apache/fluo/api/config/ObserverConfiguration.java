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

package org.apache.fluo.api.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.fluo.api.observer.Observer;

/**
 * Used to pass configuration to an {@link AbstractObserver}. Set using
 * {@link FluoConfiguration#addObserver(ObserverConfiguration)}
 */
public class ObserverConfiguration {
  private final String className;
  private Map<String, String> params = Collections.emptyMap();

  public ObserverConfiguration(String className) {
    this.className = className;
  }

  public String getClassName() {
    return className;
  }

  /**
   * For configuration that is the same across multiple observers consider using Application
   * configuration.
   *
   * @param params Parameters that should be passed to
   *        {@link Observer#init(org.apache.fluo.api.observer.Observer.Context)}
   *
   * @see FluoConfiguration#getAppConfiguration()
   */
  public ObserverConfiguration setParameters(Map<String, String> params) {
    if (params == null) {
      throw new IllegalArgumentException();
    }
    this.params = new HashMap<>(params);
    return this;
  }

  public Map<String, String> getParameters() {
    return Collections.unmodifiableMap(params);
  }

  @Override
  public int hashCode() {
    return className.hashCode() + 17 * params.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ObserverConfiguration) {
      ObserverConfiguration ooc = (ObserverConfiguration) o;
      return className.equals(ooc.className) && params.equals(ooc.params);
    }

    return false;
  }
}
