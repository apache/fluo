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

import io.fluo.api.Column;
import io.fluo.api.config.*;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

import java.io.*;
import java.util.*;


/**
 * 
 */
public class Configuration {
  
  private String table;
  private Authorizations auths = new Authorizations();
  private String zoodir;
  private String accumuloInstance;
  private Map<Column,ObserverConfiguration> observers;
  private Map<Column,ObserverConfiguration> weakObservers;
  private Set<Column> allObserversColumns;
  private Connector conn;
  private String accumuloInstanceID;
  private String fluoInstanceID;
  private int oraclePort;
  private Properties workerProps;
  private SharedResources resources;
  private long rollbackTime;
  
  public Configuration(Configuration config) throws Exception {
    this.table = config.table;
    this.auths = config.auths;
    this.zoodir = config.zoodir;
    this.accumuloInstance = config.accumuloInstance;
    this.fluoInstanceID = config.fluoInstanceID;
    this.accumuloInstanceID = config.accumuloInstanceID;
    this.observers = config.observers;
    this.conn = config.conn;

    this.resources = new SharedResources(conn.createBatchWriter(this.table, new BatchWriterConfig()), createConditionalWriter());
  }

  //TODO: This constructor will get out of control quickly
  public Configuration(ZooKeeper zk, String zoodir, Connector conn, int oraclePort) throws Exception {
    this.zoodir = zoodir;
    readConfig(zk);

    this.oraclePort = oraclePort;
    this.conn = conn;

    if (!conn.getInstance().getInstanceName().equals(accumuloInstance)) {
      throw new IllegalArgumentException("unexpected accumulo instance name " + conn.getInstance().getInstanceName() + " != " + accumuloInstance);
    }
    
    if (!conn.getInstance().getInstanceID().equals(accumuloInstanceID)) {
      throw new IllegalArgumentException("unexpected accumulo instance id " + conn.getInstance().getInstanceID() + " != " + accumuloInstanceID);
    }

    this.resources = new SharedResources(conn.createBatchWriter(this.table, new BatchWriterConfig()), createConditionalWriter());

    rollbackTime = Long.parseLong(getWorkerProperties().getProperty(TransactionConfiguration.ROLLBACK_TIME_PROP, Constants.ROLLBACK_TIME_DEFAULT + ""));
  }
  
  /**
   * @param props
   */
  public Configuration(Properties props) throws Exception {
    // TODO need to close zookeeper
    this(
      new ZooKeeper(props.getProperty(ConnectionProperties.ZOOKEEPER_CONNECT_PROP), Integer.parseInt(props.getProperty(ConnectionProperties.ZOOKEEPER_TIMEOUT_PROP)), null),
      props.getProperty(ConnectionProperties.ZOOKEEPER_ROOT_PROP),
      new ZooKeeperInstance(props.getProperty(ConnectionProperties.ACCUMULO_INSTANCE_PROP),
      props.getProperty(ConnectionProperties.ZOOKEEPER_CONNECT_PROP)).getConnector(
          props.getProperty(ConnectionProperties.ACCUMULO_USER_PROP), new PasswordToken(props.getProperty(ConnectionProperties.ACCUMULO_PASSWORD_PROP))
      ),
      Integer.parseInt(props.getProperty(OracleProperties.ORACLE_PORT_PROP, OracleProperties.ORACLE_DEFAULT_PORT + ""))
    );
  }
  
  private static Properties load(File propFile) throws FileNotFoundException, IOException {
    Properties props = new Properties(Configuration.getDefaultProperties());
    props.load(new FileReader(propFile));
    return props;
  }
  
  public Configuration(File propFile) throws Exception {
    this(load(propFile));
  }

  public static Properties getDefaultProperties() {
    Properties props = new Properties();
    props.put(ConnectionProperties.ZOOKEEPER_CONNECT_PROP, "localhost");
    props.put(ConnectionProperties.ZOOKEEPER_ROOT_PROP, "/fluo");
    props.put(ConnectionProperties.ZOOKEEPER_TIMEOUT_PROP, "30000");
    props.put(ConnectionProperties.ACCUMULO_INSTANCE_PROP, "accumulo1");
    props.put(ConnectionProperties.ACCUMULO_USER_PROP, "fluo");
    props.put(ConnectionProperties.ACCUMULO_PASSWORD_PROP, "secret");
    props.put(WorkerProperties.WORKER_INSTANCES_PROP, "1");
    props.put(WorkerProperties.WORKER_MAX_MEMORY_PROP, "256");
    props.put(OracleProperties.ORACLE_MAX_MEMORY_PROP, "256");
    props.put(OracleProperties.ORACLE_PORT_PROP, OracleProperties.ORACLE_DEFAULT_PORT+"");
    
    return props;
  }
  
  public static Properties getDefaultWorkerProperties() {
    Properties props = new Properties();
    props.put(Constants.WORKER_THREADS, "10");
    
    return props;
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
    fluoInstanceID = new String(zk.getData(zoodir + Constants.Zookeeper.FLUO_INSTANCE_ID, false, null), "UTF-8");
    table = new String(zk.getData(zoodir + Constants.Zookeeper.TABLE, false, null), "UTF-8");
    
    ByteArrayInputStream bais = new ByteArrayInputStream(zk.getData(zoodir + Constants.Zookeeper.OBSERVERS, false, null));
    DataInputStream dis = new DataInputStream(bais);
    
    observers = Collections.unmodifiableMap(readObservers(dis));
    weakObservers = Collections.unmodifiableMap(readObservers(dis));
    allObserversColumns = new HashSet<Column>();
    allObserversColumns.addAll(observers.keySet());
    allObserversColumns.addAll(weakObservers.keySet());
    allObserversColumns = Collections.unmodifiableSet(allObserversColumns);

    bais = new ByteArrayInputStream(zk.getData(zoodir + Constants.Zookeeper.WORKER_CONFIG, false, null));
    this.workerProps = new Properties(getDefaultWorkerProperties());
    this.workerProps.load(bais);
  }

  private static Map<Column,ObserverConfiguration> readObservers(DataInputStream dis) throws IOException {

    HashMap<Column,ObserverConfiguration> omap = new HashMap<Column,ObserverConfiguration>();
    
    int num = WritableUtils.readVInt(dis);
    for (int i = 0; i < num; i++) {
      Column col = new Column();
      col.readFields(dis);
      String clazz = dis.readUTF();
      Map<String,String> params = new HashMap<String,String>();
      int numParams = WritableUtils.readVInt(dis);
      for (int j = 0; j < numParams; j++) {
        String k = dis.readUTF();
        String v = dis.readUTF();
        params.put(k, v);
      }

      ObserverConfiguration observerConfig = new ObserverConfiguration(clazz);
      observerConfig.setParameters(params);

      omap.put(col, observerConfig);
    }
    
    return omap;
  }

  public void setAuthorizations(Authorizations auths) {
    this.auths = auths;

    // TODO the following is a big hack, this method is currently not exposed in API
    resources.close();
    try {
      this.resources = new SharedResources(conn.createBatchWriter(this.table, new BatchWriterConfig()), createConditionalWriter());
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
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
  
  public String getFluoInstanceID() {
    return fluoInstanceID;
  }

  public Map<Column,ObserverConfiguration> getObservers() {
    return observers;
  }

  public Map<Column,ObserverConfiguration> getWeakObservers() {
    return weakObservers;
  }

  public String getTable() {
    return table;
  }
  
  public Connector getConnector() {
    return conn;
  }
  
  public ConditionalWriter createConditionalWriter() throws TableNotFoundException {
    return conn.createConditionalWriter(table, new ConditionalWriterConfig().setAuthorizations(auths));
  }

  public SharedResources getSharedResources() {
    return resources;
  }

  public String getZookeeperRoot() {
    return zoodir;
  }
  
  public String getZookeepers() {
    return getConnector().getInstance().getZooKeepers();
  }
  
  public Properties getWorkerProperties() {
    return workerProps;
  }

  public long getRollbackTime() {
    return rollbackTime;
  }
  
  public int getOraclePort() {
    return oraclePort;
  }
}
