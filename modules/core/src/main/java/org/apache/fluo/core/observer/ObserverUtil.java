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

import java.util.Collections;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.observer.v1.ObserverStoreV1;
import org.apache.fluo.core.observer.v2.ObserverStoreV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObserverUtil {

  private static Logger logger = LoggerFactory.getLogger(ObserverUtil.class);

  public static void initialize(CuratorFramework curator, FluoConfiguration config) {

    logger.info("Setting up observers using app config: {}", config.getAppConfiguration());

    ObserverStore ov1 = new ObserverStoreV1();
    ObserverStore ov2 = new ObserverStoreV2();

    if (ov1.handles(config) && ov2.handles(config)) {
      throw new IllegalArgumentException(
          "Old and new observers configuration present.  There can only be one.");
    }

    try {
      if (ov1.handles(config)) {
        ov2.clear(curator);
        ov1.update(curator, config);
      } else if (ov2.handles(config)) {
        ov1.clear(curator);
        ov2.update(curator, config);
      }
    } catch (Exception e) {
      throw new FluoException("Failed to update shared configuration in Zookeeper", e);
    }
  }

  public static RegisteredObservers load(CuratorFramework curator) throws Exception {
    ObserverStore ov1 = new ObserverStoreV1();
    ObserverStore ov2 = new ObserverStoreV2();

    // try to load observers using old and new config
    RegisteredObservers co = ov1.load(curator);
    if (co == null) {
      co = ov2.load(curator);
    }

    if (co == null) {
      // no observers configured, so return an empty provider
      co = new RegisteredObservers() {
        @Override
        public Observers getObservers(Environment env) {
          return new Observers() {

            @Override
            public void close() {

            }

            @Override
            public void returnObserver(Observer o) {
              throw new UnsupportedOperationException();
            }

            @Override
            public Observer getObserver(Column col) {
              throw new UnsupportedOperationException();
            }

            @Override
            public String getObserverId(Column col) {
              throw new UnsupportedOperationException();
            }
          };
        }

        @Override
        public Set<Column> getObservedColumns(NotificationType nt) {
          return Collections.emptySet();
        }
      };
    }

    return co;
  }
}
