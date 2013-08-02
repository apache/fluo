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
package org.apache.accumulo.accismus;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.accismus.format.AccismusFormatter;
import org.apache.accumulo.accismus.impl.ByteUtil;
import org.apache.accumulo.accismus.iterators.GarbageCollectionIterator;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 */
public class Operations {
  
  public static void initialize(Connector conn, String zoodir, String table, Map<Column,String> colObservers) throws Exception {

    String zookeepers = conn.getInstance().getZooKeepers();
    String accumuloInstanceName = conn.getInstance().getInstanceName();
    String accumuloInstanceID = conn.getInstance().getInstanceID();
    String accismusInstanceID = UUID.randomUUID().toString();

    ZooKeeper zk = new ZooKeeper(zookeepers, 30000, null);
    
    // TODO set Accismus data version

    zk.create(zoodir, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(zoodir + Constants.Zookeeper.CONFIG, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(zoodir + Constants.Zookeeper.TABLE, table.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(zoodir + Constants.Zookeeper.ACCUMULO_INSTANCE_NAME, accumuloInstanceName.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(zoodir + Constants.Zookeeper.ACCUMULO_INSTANCE_ID, accumuloInstanceID.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(zoodir + Constants.Zookeeper.ACCISMUS_INSTANCE_ID, accismusInstanceID.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    zk.create(zoodir + Constants.Zookeeper.ORACLE, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    zk.create(zoodir + Constants.Zookeeper.TIMESTAMP, new byte[] {'0'}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    
    Set<Entry<Column,String>> es = colObservers.entrySet();
    
    WritableUtils.writeVInt(dos, colObservers.size());
    
    for (Entry<Column,String> entry : es) {
      entry.getKey().write(dos);
      dos.writeUTF(entry.getValue());
    }
    
    dos.close();
    
    zk.create(zoodir + Constants.Zookeeper.OBSERVERS, baos.toByteArray(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    
    createTable(table, conn);
  }

  private static void createTable(String tableName, Connector conn) throws Exception {
    conn.tableOperations().create(tableName, false);
    Map<String,Set<Text>> groups = new HashMap<String,Set<Text>>();
    groups.put("notify", Collections.singleton(ByteUtil.toText(Constants.NOTIFY_CF)));
    conn.tableOperations().setLocalityGroups(tableName, groups);
    
    IteratorSetting gcIter = new IteratorSetting(10, GarbageCollectionIterator.class);
    GarbageCollectionIterator.setNumVersions(gcIter, 2);
    
    conn.tableOperations().attachIterator(tableName, gcIter, EnumSet.of(IteratorScope.majc, IteratorScope.minc));
    
    conn.tableOperations().setProperty(tableName, Property.TABLE_FORMATTER_CLASS.getKey(), AccismusFormatter.class.getName());
  }
}
