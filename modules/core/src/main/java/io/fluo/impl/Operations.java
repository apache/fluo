/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.impl;

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

import io.fluo.api.Column;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.core.util.CuratorUtil;
import io.fluo.format.FluoFormatter;
import io.fluo.impl.iterators.GarbageCollectionIterator;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeExistsPolicy;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 *
 */
public class Operations {

  private static boolean putData(CuratorFramework curator, String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    if (policy == null)
      policy = NodeExistsPolicy.FAIL;

    while (true) {
      try {
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(zPath, data);
        return true;
      } catch (Exception nee) {
        if(nee instanceof NodeExistsException) {
          switch (policy) {
            case SKIP:
              return false;
            case OVERWRITE:
              try {
                curator.setData().withVersion(-1).forPath(zPath, data);
                return true;
              } catch (Exception nne) {

                if(nne instanceof NoNodeException)
                  // node delete between create call and set data, so try create call again
                  continue;
                else
                  throw new RuntimeException(nne);
              }
            default:
              throw (NodeExistsException)nee;
          }
        } else
          throw new RuntimeException(nee);
      }
    }
  }

  // TODO refactor all method in this class to take a properties object... if so the prop keys would need to be public

  public static void updateWorkerConfig(Connector conn, String zoodir, Properties workerConfig) throws Exception {

    String zookeepers = conn.getInstance().getZooKeepers();
    try (CuratorFramework curator = CuratorUtil.getCurator(zookeepers, 30000)) {

      curator.start();

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      workerConfig.store(baos, "Java props");

      putData(curator, Constants.workerConfigPath(zoodir), baos.toByteArray(), NodeExistsPolicy.OVERWRITE);
    }
  }

  public static void updateObservers(Connector conn, String zoodir, Map<Column,ObserverConfiguration> colObservers,
      Map<Column,ObserverConfiguration> weakObservers) throws Exception {

    // TODO check that no workers are running... or make workers watch this znode
    String zookeepers = conn.getInstance().getZooKeepers();
    try (CuratorFramework curator = CuratorUtil.getCurator(zookeepers, 30000)) {
      curator.start();

      ZooKeeper zk = curator.getZookeeperClient().getZooKeeper();

      ZooUtil.recursiveDelete(zk, Constants.observersPath(zoodir), NodeMissingPolicy.SKIP);

      byte[] serializedObservers = serializeObservers(colObservers, weakObservers);
      putData(curator, Constants.observersPath(zoodir), serializedObservers, NodeExistsPolicy.OVERWRITE);
    }
  }

  public static void initialize(Connector conn, String zoodir, String table) throws Exception {

    String zookeepers = conn.getInstance().getZooKeepers();
    String accumuloInstanceName = conn.getInstance().getInstanceName();
    String accumuloInstanceID = conn.getInstance().getInstanceID();
    String fluoInstanceID = UUID.randomUUID().toString();

    try (CuratorFramework curator = CuratorUtil.getCurator(zookeepers, 30000)) {
      curator.start();

      // TODO set Fluo data version
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(zoodir, new byte[0]);
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(Constants.configPath(zoodir), new byte[0]);
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(Constants.tablePath(zoodir), table.getBytes("UTF-8"));
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(Constants.instanceNamePath(zoodir), accumuloInstanceName.getBytes("UTF-8"));
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(Constants.accumuloInstanceIdPath(zoodir), accumuloInstanceID.getBytes("UTF-8"));
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(Constants.fluoInstanceIdPath(zoodir), fluoInstanceID.getBytes("UTF-8"));

      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(Constants.oraclePath(zoodir), new byte[0]);
      curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(Constants.timestampPath(zoodir), new byte[] {'2'});

      createTable(table, conn);
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

  private static void createTable(String tableName, Connector conn) throws Exception {
    // TODO may need to configure an iterator that squishes multiple notifications to one at compaction time since versioning iterator is not configured for
    // table...

    conn.tableOperations().create(tableName, false);
    Map<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
    groups.put("notify", Collections.singleton(ByteUtil.toText(Constants.NOTIFY_CF)));
    conn.tableOperations().setLocalityGroups(tableName, groups);
    
    IteratorSetting gcIter = new IteratorSetting(10, GarbageCollectionIterator.class);
    GarbageCollectionIterator.setNumVersions(gcIter, 2);
    
    conn.tableOperations().attachIterator(tableName, gcIter, EnumSet.of(IteratorScope.majc, IteratorScope.minc));
    
    conn.tableOperations().setProperty(tableName, Property.TABLE_FORMATTER_CLASS.getKey(), FluoFormatter.class.getName());
  }

}
