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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.ObserversFactory;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.observer.ConfiguredObservers;
import org.apache.fluo.core.observer.ObserverProvider;
import org.apache.fluo.core.observer.Observers;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.zookeeper.KeeperException.NoNodeException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.fluo.accumulo.util.ZookeeperPath.CONFIG_FLUO_OBSERVERS2;

/*
 * Support for observers configured the new way.
 */
public class ObserversV2 implements Observers {

  @Override
  public boolean handles(FluoConfiguration config) {
    return !config.getObserversFactory().isEmpty();
  }

  @Override
  public void update(CuratorFramework curator, FluoConfiguration config) throws Exception {
    String obsFactoryClass = config.getObserversFactory();

    ObserversFactory observerFactory = newObserversFactory(obsFactoryClass);

    Map<Column, NotificationType> obsCols = new HashMap<>();
    BiConsumer<Column, NotificationType> obsColConsumer = (col, nt) -> {
      Objects.requireNonNull(col, "Observed column must be non-null");
      Objects.requireNonNull(nt, "Notification type must be non-null");
      Preconditions.checkArgument(!obsCols.containsKey(col), "Duplicate observed column %s", col);
      obsCols.put(col, nt);
    };

    observerFactory.getObservedColumns(
        new ObserverFactoryContextImpl(config.getAppConfiguration()), obsColConsumer);

    // TODO does core depend on GSon? If not, do we want to add dep?
    Gson gson = new Gson();
    String json = gson.toJson(new JsonObservers(obsFactoryClass, obsCols));
    CuratorUtil.putData(curator, CONFIG_FLUO_OBSERVERS2, json.getBytes(UTF_8),
        CuratorUtil.NodeExistsPolicy.OVERWRITE);

  }

  static ObserversFactory newObserversFactory(String obsFactoryClass) {
    ObserversFactory observerFactory;
    try {
      observerFactory =
          Class.forName(obsFactoryClass).asSubclass(ObserversFactory.class).newInstance();
    } catch (ClassNotFoundException e1) {
      throw new FluoException("ObserverFactory class '" + obsFactoryClass + "' was not "
          + "found.  Check for class name misspellings or failure to include "
          + "the observer factory jar.", e1);
    } catch (InstantiationException | IllegalAccessException e2) {
      throw new FluoException("ObserverFactory class '" + obsFactoryClass
          + "' could not be created.", e2);
    }
    return observerFactory;
  }

  @Override
  public ConfiguredObservers load(CuratorFramework curator) throws Exception {
    byte[] data;
    try {
      data = curator.getData().forPath(CONFIG_FLUO_OBSERVERS2);
    } catch (NoNodeException nne) {
      return null;
    }
    String json = new String(data, UTF_8);
    JsonObservers jco = new Gson().fromJson(json, JsonObservers.class);

    Set<Column> weakColumns = new HashSet<>();
    Set<Column> strongColumns = new HashSet<>();

    for (Entry<Column, NotificationType> entry : jco.getObservedColumns().entrySet()) {
      switch (entry.getValue()) {
        case STRONG:
          strongColumns.add(entry.getKey());
          break;
        case WEAK:
          weakColumns.add(entry.getKey());
          break;
        default:
          throw new IllegalStateException("Unknown notification type " + entry.getValue());
      }
    }

    return new ConfiguredObservers() {

      @Override
      public ObserverProvider getProvider(Environment env) {
        return new ObserversProviderV2(env, jco, strongColumns, weakColumns);
      }

      @Override
      public Set<Column> getObservedColumns(NotificationType nt) {
        switch (nt) {
          case STRONG:
            return strongColumns;
          case WEAK:
            return weakColumns;
          default:
            throw new IllegalArgumentException("Unknown notification type " + nt);
        }
      }
    };
  }

  @Override
  public void clear(CuratorFramework curator) throws Exception {
    try {
      curator.delete().forPath(CONFIG_FLUO_OBSERVERS2);
    } catch (NoNodeException nne) {
      // nothing to delete
    }
  }

}
