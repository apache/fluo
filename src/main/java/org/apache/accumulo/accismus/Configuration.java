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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;


/**
 * 
 */
public class Configuration {
  
  private String table;
  private Authorizations auths = new Authorizations();
  private String zoodir;
  private String accumuloInstance;
  private Map<Column,String> observers;
  private Connector conn;
  private String accumuloInstanceID;
  private String accismusInstanceID;
  
  public Configuration(Configuration config) throws Exception {
    this.table = config.table;
    this.auths = config.auths;
    this.zoodir = config.zoodir;
    this.accumuloInstance = config.accumuloInstance;
    this.accismusInstanceID = config.accismusInstanceID;
    this.accumuloInstanceID = config.accumuloInstanceID;
    this.observers = config.observers;
    this.conn = config.conn;
  }

  public Configuration(ZooKeeper zk, String zoodir, Connector conn) throws Exception {
    this.zoodir = zoodir;
    readConfig(zk);
    
    this.conn = conn;

    if (!conn.getInstance().getInstanceName().equals(accumuloInstance)) {
      throw new IllegalArgumentException("unexpected accumulo instance name " + conn.getInstance().getInstanceName() + " != " + accumuloInstance);
    }
    
    if (!conn.getInstance().getInstanceID().equals(accumuloInstanceID)) {
      throw new IllegalArgumentException("unexpected accumulo instance id " + conn.getInstance().getInstanceID() + " != " + accumuloInstanceID);
    }
  }
  
  /**
   * read configuration from zookeeper
   * 
   * @throws InterruptedException
   * @throws KeeperException
   */
  private void readConfig(ZooKeeper zk) throws Exception {
    accumuloInstance = new String(zk.getData(zoodir + Constants.Zookeeper.ACCUMULO_INSTANCE_NAME, false, null), "UTF-8");
    accumuloInstanceID = new String(zk.getData(zoodir + Constants.Zookeeper.ACCUMULO_INSTANCE_ID, false, null), "UTF-8");
    accismusInstanceID = new String(zk.getData(zoodir + Constants.Zookeeper.ACCISMUS_INSTANCE_ID, false, null), "UTF-8");
    table = new String(zk.getData(zoodir + Constants.Zookeeper.TABLE, false, null), "UTF-8");
    
    ByteArrayInputStream bais = new ByteArrayInputStream(zk.getData(zoodir + Constants.Zookeeper.OBSERVERS, false, null));
    DataInputStream dis = new DataInputStream(bais);
    
    observers = new HashMap<Column,String>();
    
    int num = WritableUtils.readVInt(dis);
    for (int i = 0; i < num; i++) {
      Column col = new Column();
      col.readFields(dis);
      String clazz = dis.readUTF();
      observers.put(col, clazz);
    }

    observers = Collections.unmodifiableMap(observers);

  }

  public void setAuthorizations(Authorizations auths) {
    this.auths = auths;
  }
  
  public Authorizations getAuthorizations() {
    return auths;
  }

  public String getAccumuloInstance() {
    return accumuloInstance;
  }
  
  public String getAccumuloInstanceID() {
    return accumuloInstanceID;
  }
  
  public String getAccismusInstanceID() {
    return accismusInstanceID;
  }

  public Map<Column,String> getObservers() {
    return observers;
  }

  public String getTable() {
    return table;
  }
  
  public Connector getConnector() {
    return conn;
  }
  
  public ConditionalWriter createConditionalWriter() throws TableNotFoundException {
    return conn.createConditionalWriter(table, auths);
  }

  public String getZookeeperRoot() {
    return zoodir;
  }
}
