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

package org.apache.fluo.core.client;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.core.util.ColumnUtil;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for initializing Zookeeper and Accumulo
 */
public class Operations {

  private Operations() {}

  private static final Logger logger = LoggerFactory.getLogger(Operations.class);

  // TODO refactor all method in this class to take a properties object... if so the prop keys would
  // need to be public

  public static void updateSharedConfig(CuratorFramework curator, Properties sharedProps)
      throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    sharedProps.store(baos, "Shared java props");

    CuratorUtil.putData(curator, ZookeeperPath.CONFIG_SHARED, baos.toByteArray(),
        CuratorUtil.NodeExistsPolicy.OVERWRITE);
  }

  public static void updateObservers(CuratorFramework curator,
      Map<Column, ObserverSpecification> colObservers,
      Map<Column, ObserverSpecification> weakObservers) throws Exception {

    // TODO check that no workers are running... or make workers watch this znode

    String observerPath = ZookeeperPath.CONFIG_FLUO_OBSERVERS;
    try {
      curator.delete().deletingChildrenIfNeeded().forPath(observerPath);
    } catch (NoNodeException nne) {
      // it's ok if node doesn't exist
    } catch (Exception e) {
      logger.error("An error occurred deleting Zookeeper node. node=[" + observerPath
          + "], error=[" + e.getMessage() + "]");
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
}
