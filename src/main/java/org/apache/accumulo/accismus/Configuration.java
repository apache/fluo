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

import org.apache.accumulo.core.client.Connector;
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
  
  public Configuration(ZooKeeper zk, String zoodir, Connector conn) throws Exception {
    this.zoodir = zoodir;
    readConfig(zk);
    
    this.conn = conn;

    if (!conn.getInstance().getInstanceName().equals(accumuloInstance)) {
      throw new IllegalArgumentException("unexpected accumulo instance name " + conn.getInstance().getInstanceName() + " != " + accumuloInstance);
    }
  }
  
  /**
   * read configuration from zookeeper
   * 
   * @throws InterruptedException
   * @throws KeeperException
   */
  private void readConfig(ZooKeeper zk) throws Exception {
    accumuloInstance = new String(zk.getData(zoodir + Constants.Zookeeper.ACCUMULO_INSTANCE, false, null), "UTF-8");
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
  
  public Map<Column,String> getObservers() {
    return observers;
  }

  public String getTable() {
    return table;
  }
  
  public Connector getConnector() {
    return conn;
  }
}
