/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.api.config;

import java.util.Collections;
import java.util.Map;

import io.fluo.api.observer.AbstractObserver;
import io.fluo.api.observer.Observer;

/**
 * Used to pass configuration to an {@link AbstractObserver}. Set using
 * {@link FluoConfiguration#setObservers(java.util.List)}
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
   * For configuration thats the same across multiple observers consider using Application
   * configuration.
   * 
   * @param params Parameters that should be passed to
   *        {@link Observer#init(io.fluo.api.observer.Observer.Context)}
   * @return
   * 
   * @see FluoConfiguration#getAppConfiguration()
   */
  public ObserverConfiguration setParameters(Map<String, String> params) {
    if (params == null) {
      throw new IllegalArgumentException();
    }
    this.params = params;
    return this;
  }

  public Map<String, String> getParameters() {
    return params;
  }

}
