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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.fluo.api.observer.Observer;

/**
 * This class encapsulates the information needed to setup an Observer. This class is used by
 * {@link FluoConfiguration#addObserver(ObserverSpecification)}.
 *
 * @since 1.0.0
 * @deprecated since 1.1.0. The methods that used this class in {@link FluoConfiguration} were
 *             deprecated.
 */
@Deprecated
public class ObserverSpecification {
  private final String className;
  private final Map<String, String> configMap;
  private SimpleConfiguration config = null;

  /**
   * @param className The name of a class that implements {@link Observer}
   */
  public ObserverSpecification(String className) {
    this.className = className;
    this.configMap = Collections.emptyMap();
  }

  /**
   * @param className The name of a class that implements {@link Observer}
   * @param observerConfig Per observer configuration thats specific to this observer. For
   *        configuration thats the same across multiple observers, consider using
   *        {@link FluoConfiguration#getAppConfiguration()}
   */
  public ObserverSpecification(String className, SimpleConfiguration observerConfig) {
    this.className = className;
    this.configMap = observerConfig.toMap();
  }

  /**
   * @param className The name of a class that implements {@link Observer}
   * @param observerConfig Per observer configuration thats specific to this observer. For
   *        configuration thats the same across multiple observers, consider using
   *        {@link FluoConfiguration#getAppConfiguration()}
   */
  public ObserverSpecification(String className, Map<String, String> observerConfig) {
    this.className = className;
    this.configMap = ImmutableMap.copyOf(observerConfig);
  }

  public String getClassName() {
    return className;
  }

  public SimpleConfiguration getConfiguration() {
    if (config == null) {
      config = new SimpleConfiguration(configMap);
    }
    return config;
  }

  @Override
  public int hashCode() {
    return className.hashCode() + 17 * configMap.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof ObserverSpecification) {
      ObserverSpecification ooc = (ObserverSpecification) o;
      return className.equals(ooc.className) && configMap.equals(ooc.configMap);
    }

    return false;
  }
}
