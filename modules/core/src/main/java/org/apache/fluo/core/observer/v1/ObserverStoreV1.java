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

package org.apache.fluo.core.observer.v1;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.api.observer.Observer.NotificationType;
import org.apache.fluo.api.observer.Observer.ObservedColumn;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.observer.ObserverStore;
import org.apache.fluo.core.observer.Observers;
import org.apache.fluo.core.observer.RegisteredObservers;
import org.apache.fluo.core.util.ColumnUtil;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Support for observers configured the old way.
 */
@SuppressWarnings("deprecation")
public class ObserverStoreV1 implements ObserverStore {

  private static final Logger logger = LoggerFactory.getLogger(ObserverStoreV1.class);

  @Override
  public boolean handles(FluoConfiguration config) {
    Collection<ObserverSpecification> obsSpecs = config.getObserverSpecifications();
    return !obsSpecs.isEmpty();
  }

  @Override
  public void update(CuratorFramework curator, FluoConfiguration config) throws Exception {
    Collection<ObserverSpecification> obsSpecs = config.getObserverSpecifications();

    Map<Column, ObserverSpecification> colObservers = new HashMap<>();
    Map<Column, ObserverSpecification> weakObservers = new HashMap<>();

    for (ObserverSpecification ospec : obsSpecs) {
      Observer observer;
      try {
        observer = Class.forName(ospec.getClassName()).asSubclass(Observer.class).newInstance();
      } catch (ClassNotFoundException e1) {
        throw new FluoException("Observer class '" + ospec.getClassName() + "' was not "
            + "found.  Check for class name misspellings or failure to include "
            + "the observer jar.", e1);
      } catch (InstantiationException | IllegalAccessException e2) {
        throw new FluoException(
            "Observer class '" + ospec.getClassName() + "' could not be created.", e2);
      }

      SimpleConfiguration oc = ospec.getConfiguration();
      logger.info("Setting up observer {} using params {}.", observer.getClass().getSimpleName(),
          oc.toMap());
      try {
        observer.init(new ObserverContext(config.getAppConfiguration(), oc));
      } catch (Exception e) {
        throw new FluoException("Observer '" + ospec.getClassName() + "' could not be initialized",
            e);
      }

      ObservedColumn observedCol = observer.getObservedColumn();
      if (observedCol.getType() == NotificationType.STRONG) {
        colObservers.put(observedCol.getColumn(), ospec);
      } else {
        weakObservers.put(observedCol.getColumn(), ospec);
      }
    }

    updateObservers(curator, colObservers, weakObservers);
  }

  private static void updateObservers(CuratorFramework curator,
      Map<Column, ObserverSpecification> colObservers,
      Map<Column, ObserverSpecification> weakObservers) throws Exception {

    // TODO check that no workers are running... or make workers watch this znode

    String observerPath = ZookeeperPath.CONFIG_FLUO_OBSERVERS1;
    try {
      curator.delete().deletingChildrenIfNeeded().forPath(observerPath);
    } catch (NoNodeException nne) {
      // it's ok if node doesn't exist
    } catch (Exception e) {
      logger.error("An error occurred deleting Zookeeper node. node=[" + observerPath + "], error=["
          + e.getMessage() + "]");
      throw new RuntimeException(e);
    }

    byte[] serializedObservers = serializeObservers(colObservers, weakObservers);
    CuratorUtil.putData(curator, observerPath, serializedObservers,
        CuratorUtil.NodeExistsPolicy.OVERWRITE);
  }

  private static void serializeObservers(DataOutputStream dos,
      Map<Column, ObserverSpecification> colObservers) throws IOException {
    // TODO use a human readable serialized format like json

    Set<Entry<Column, ObserverSpecification>> es = colObservers.entrySet();

    WritableUtils.writeVInt(dos, colObservers.size());

    for (Entry<Column, ObserverSpecification> entry : es) {
      ColumnUtil.writeColumn(entry.getKey(), dos);
      dos.writeUTF(entry.getValue().getClassName());
      Map<String, String> params = entry.getValue().getConfiguration().toMap();
      WritableUtils.writeVInt(dos, params.size());
      for (Entry<String, String> pentry : params.entrySet()) {
        dos.writeUTF(pentry.getKey());
        dos.writeUTF(pentry.getValue());
      }
    }
  }

  private static byte[] serializeObservers(Map<Column, ObserverSpecification> colObservers,
      Map<Column, ObserverSpecification> weakObservers) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      serializeObservers(dos, colObservers);
      serializeObservers(dos, weakObservers);
    }

    byte[] serializedObservers = baos.toByteArray();
    return serializedObservers;
  }


  private static Map<Column, ObserverSpecification> readObservers(DataInputStream dis)
      throws IOException {

    ImmutableMap.Builder<Column, ObserverSpecification> omapBuilder =
        new ImmutableMap.Builder<Column, ObserverSpecification>();

    int num = WritableUtils.readVInt(dis);
    for (int i = 0; i < num; i++) {
      Column col = ColumnUtil.readColumn(dis);
      String clazz = dis.readUTF();
      Map<String, String> params = new HashMap<>();
      int numParams = WritableUtils.readVInt(dis);
      for (int j = 0; j < numParams; j++) {
        String k = dis.readUTF();
        String v = dis.readUTF();
        params.put(k, v);
      }

      ObserverSpecification ospec = new ObserverSpecification(clazz, params);
      omapBuilder.put(col, ospec);
    }
    return omapBuilder.build();
  }

  @Override
  public RegisteredObservers load(CuratorFramework curator) throws Exception {

    Map<Column, ObserverSpecification> observers;
    Map<Column, ObserverSpecification> weakObservers;

    ByteArrayInputStream bais;
    try {
      bais =
          new ByteArrayInputStream(curator.getData().forPath(ZookeeperPath.CONFIG_FLUO_OBSERVERS1));
    } catch (NoNodeException nne) {
      return null;
    }
    DataInputStream dis = new DataInputStream(bais);

    observers = readObservers(dis);
    weakObservers = readObservers(dis);


    return new RegisteredObservers() {

      @Override
      public Observers getObservers(Environment env) {
        return new ObserversV1(env, observers, weakObservers);
      }

      @Override
      public Set<Column> getObservedColumns(NotificationType nt) {
        switch (nt) {
          case STRONG:
            return observers.keySet();
          case WEAK:
            return weakObservers.keySet();
          default:
            throw new IllegalArgumentException("Unknown notification type " + nt);
        }
      }
    };
  }

  @Override
  public void clear(CuratorFramework curator) throws Exception {
    try {
      curator.delete().forPath(ZookeeperPath.CONFIG_FLUO_OBSERVERS1);
    } catch (NoNodeException nne) {
      // nothing to delete
    }
  }
}
