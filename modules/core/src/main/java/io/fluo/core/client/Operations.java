/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.core.client;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import io.fluo.accumulo.format.FluoFormatter;
import io.fluo.accumulo.iterators.GarbageCollectionIterator;
import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.util.ZookeeperPath;
import io.fluo.accumulo.util.ZookeeperUtil;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Column;
import io.fluo.core.util.ByteUtil;
import io.fluo.core.util.CuratorUtil;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for initializing Zookeeper & Accumulo
 */
public class Operations {
  
  private Operations() {}

  private static final Logger logger = LoggerFactory.getLogger(Operations.class);

  // TODO refactor all method in this class to take a properties object... if so the prop keys would need to be public

  public static void updateSharedConfig(FluoConfiguration config, Connector conn, Properties sharedProps) throws Exception {

    try (CuratorFramework curator = CuratorUtil.getCurator(config.getZookeepers(), 30000)) {

      curator.start();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      sharedProps.store(baos, "Shared java props");

      CuratorUtil.putData(curator, ZookeeperPath.CONFIG_SHARED, baos.toByteArray(), CuratorUtil.NodeExistsPolicy.OVERWRITE);
    }
  }

  public static void updateObservers(FluoConfiguration config, Connector conn, Map<Column,ObserverConfiguration> colObservers,
      Map<Column,ObserverConfiguration> weakObservers) throws Exception {

    // TODO check that no workers are running... or make workers watch this znode
    try (CuratorFramework curator = CuratorUtil.getCurator(config.getZookeepers(), 30000)) {
      curator.start();

      String observerPath = ZookeeperPath.CONFIG_FLUO_OBSERVERS;
      try {
        curator.delete().deletingChildrenIfNeeded().forPath(observerPath);
      } catch(NoNodeException nne) {
      } catch(Exception e) {
        logger.error("An error occurred deleting Zookeeper node. node=[" + observerPath + "], error=[" + e.getMessage() + "]");
        throw new RuntimeException(e);
      }

      byte[] serializedObservers = serializeObservers(colObservers, weakObservers);
      CuratorUtil.putData(curator, observerPath, serializedObservers, CuratorUtil.NodeExistsPolicy.OVERWRITE);
    }
  }

  public static void initialize(FluoConfiguration config, Connector conn) throws Exception {

    final String accumuloInstanceName = conn.getInstance().getInstanceName();
    final String accumuloInstanceID = conn.getInstance().getInstanceID();
    final String fluoInstanceID = UUID.randomUUID().toString();
    
    // Create node specified by chroot suffix of Zookeeper connection string (if it doesn't exist)
    try (CuratorFramework curator = CuratorUtil.getCurator(ZookeeperUtil.parseServers(config.getZookeepers()), 30000)) {
      curator.start();
      String zkRoot = ZookeeperUtil.parseRoot(config.getZookeepers());
      CuratorUtil.putData(curator, zkRoot, new byte[0], CuratorUtil.NodeExistsPolicy.FAIL);
    }

    // Initialize Zookeeper & Accumulo for this Fluo instance
    try (CuratorFramework curator = CuratorUtil.getCurator(config.getZookeepers(), 30000)) {
      curator.start();

      // TODO set Fluo data version
      CuratorUtil.putData(curator, ZookeeperPath.CONFIG, new byte[0], CuratorUtil.NodeExistsPolicy.FAIL);
      CuratorUtil.putData(curator, ZookeeperPath.CONFIG_ACCUMULO_TABLE, config.getAccumuloTable().getBytes("UTF-8"), CuratorUtil.NodeExistsPolicy.FAIL);
      CuratorUtil.putData(curator, ZookeeperPath.CONFIG_ACCUMULO_INSTANCE_NAME, accumuloInstanceName.getBytes("UTF-8"), CuratorUtil.NodeExistsPolicy.FAIL);
      CuratorUtil.putData(curator, ZookeeperPath.CONFIG_ACCUMULO_INSTANCE_ID, accumuloInstanceID.getBytes("UTF-8"), CuratorUtil.NodeExistsPolicy.FAIL);
      CuratorUtil.putData(curator, ZookeeperPath.CONFIG_FLUO_INSTANCE_ID, fluoInstanceID.getBytes("UTF-8"), CuratorUtil.NodeExistsPolicy.FAIL);
      CuratorUtil.putData(curator, ZookeeperPath.ORACLE_SERVER, new byte[0], CuratorUtil.NodeExistsPolicy.FAIL);
      CuratorUtil.putData(curator, ZookeeperPath.ORACLE_MAX_TIMESTAMP, new byte[] {'2'}, CuratorUtil.NodeExistsPolicy.FAIL);
      CuratorUtil.putData(curator, ZookeeperPath.ORACLE_CUR_TIMESTAMP, new byte[] {'0'}, CuratorUtil.NodeExistsPolicy.FAIL);

      createTable(config, conn);
    }
  }

  private static void serializeObservers(DataOutputStream dos, Map<Column,ObserverConfiguration> colObservers) throws IOException {
    // TODO use a human readable serialized format like json

    Set<Entry<Column,ObserverConfiguration>> es = colObservers.entrySet();

    WritableUtils.writeVInt(dos, colObservers.size());

    for (Entry<Column,ObserverConfiguration> entry : es) {
      entry.getKey().write(dos);
      dos.writeUTF(entry.getValue().getClassName());
      Map<String,String> params = entry.getValue().getParameters();
      WritableUtils.writeVInt(dos, params.size());
      for (Entry<String,String> pentry : entry.getValue().getParameters().entrySet()) {
        dos.writeUTF(pentry.getKey());
        dos.writeUTF(pentry.getValue());
      }
    }
  }

  private static byte[] serializeObservers(Map<Column,ObserverConfiguration> colObservers, Map<Column,ObserverConfiguration> weakObservers) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (DataOutputStream dos = new DataOutputStream(baos)) {
      serializeObservers(dos, colObservers);
      serializeObservers(dos, weakObservers);
    }
    
    byte[] serializedObservers = baos.toByteArray();
    return serializedObservers;
  }

  private static void createTable(FluoConfiguration config, Connector conn) throws Exception {
    // TODO may need to configure an iterator that squishes multiple notifications to one at compaction time since versioning iterator is not configured for
    // table...

    conn.tableOperations().create(config.getAccumuloTable(), false);
    Map<String,Set<Text>> groups = new HashMap<>();
    groups.put("notify", Collections.singleton(ByteUtil.toText(ColumnConstants.NOTIFY_CF)));
    conn.tableOperations().setLocalityGroups(config.getAccumuloTable(), groups);
    
    IteratorSetting gcIter = new IteratorSetting(10, GarbageCollectionIterator.class);
    GarbageCollectionIterator.setZookeepers(gcIter, config.getZookeepers());
    
    conn.tableOperations().attachIterator(config.getAccumuloTable(), gcIter, EnumSet.of(IteratorScope.majc, IteratorScope.minc));
    
    conn.tableOperations().setProperty(config.getAccumuloTable(), Property.TABLE_FORMATTER_CLASS.getKey(), FluoFormatter.class.getName());
  }
}
