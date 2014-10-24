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
package io.fluo.core.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import io.fluo.accumulo.util.ZookeeperPath;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Column;
import io.fluo.core.util.CuratorUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.io.WritableUtils;
import org.apache.zookeeper.KeeperException;

/**
 * Holds common environment configuration and shared resources
 */
public class Environment implements AutoCloseable {
  
  private String table;
  private Authorizations auths = new Authorizations();
  private String accumuloInstance;
  private Map<Column,ObserverConfiguration> observers;
  private Map<Column,ObserverConfiguration> weakObservers;
  private Set<Column> allObserversColumns;
  private Connector conn;
  private String accumuloInstanceID;
  private String fluoInstanceID;
  private int oraclePort;
  private FluoConfiguration config;
  private SharedResources resources;
  private long rollbackTime;
  
  /**
   * Constructs an environment from another environment
   * @param env
   * @throws Exception
   */
  public Environment(Environment env) throws Exception {
    this.table = env.table;
    this.auths = env.auths;
    this.accumuloInstance = env.accumuloInstance;
    this.observers = env.observers;
    this.weakObservers = env.weakObservers;
    this.allObserversColumns = env.allObserversColumns;
    this.conn = env.conn;
    this.accumuloInstanceID = env.accumuloInstanceID;
    this.fluoInstanceID = env.fluoInstanceID;
    this.observers = env.observers;
    this.oraclePort = env.oraclePort;
    this.config = env.config;
    this.resources = new SharedResources(this);
    this.rollbackTime = env.rollbackTime;
  }

  /**
   * Constructs an environment from given FluoConfiguration
   * @param configuration Configuration used to configure environment
   */
  public Environment(FluoConfiguration configuration) {
    this.config = configuration;
    
    try (CuratorFramework curator = CuratorUtil.getCurator(config.getZookeepers(), config.getZookeeperTimeout())) {

      Connector conn;
      try {
        conn = new ZooKeeperInstance(config.getAccumuloInstance(), config.getAccumuloZookeepers())
          .getConnector(config.getAccumuloUser(), new PasswordToken(config.getAccumuloPassword()));
      } catch (AccumuloException | AccumuloSecurityException e) {
        throw new IllegalStateException(e);
      }

      curator.start();

      init(curator, conn, config.getOraclePort());
    }
  }
    
  /**
   * Constructs environment from fluo.properties file
   * @param propFile fluo.properties File
   * @throws Exception
   */
  public Environment(File propFile) throws Exception {
    this(new FluoConfiguration(propFile));
  }
  
  @VisibleForTesting
  public Environment(FluoConfiguration config, CuratorFramework curator, Connector conn, int oraclePort) throws Exception {
    this.config = config;
    init(curator, conn, oraclePort);
  }
  
  private void init(CuratorFramework curator, Connector conn, int oraclePort) {
    try {
      readConfig(curator);
    } catch (Exception e1) {
      throw new IllegalStateException(e1);
    }

    this.oraclePort = oraclePort;
    this.conn = conn;

    if (!conn.getInstance().getInstanceName().equals(accumuloInstance))
      throw new IllegalArgumentException("unexpected accumulo instance name " + conn.getInstance().getInstanceName() + " != " + accumuloInstance);

    if (!conn.getInstance().getInstanceID().equals(accumuloInstanceID))
      throw new IllegalArgumentException("unexpected accumulo instance id " + conn.getInstance().getInstanceID() + " != " + accumuloInstanceID);

    rollbackTime = config.getTransactionRollbackTime();

    try {
      this.resources = new SharedResources(this);
    } catch (TableNotFoundException e2) {
      throw new IllegalStateException(e2);
    }
  }
  
  /**
   * read configuration from zookeeper
   * 
   * @throws InterruptedException
   * @throws KeeperException
   */
  private void readConfig(CuratorFramework curator) throws Exception {
    
    accumuloInstance = new String(curator.getData().forPath(ZookeeperPath.CONFIG_ACCUMULO_INSTANCE_NAME), "UTF-8");
    accumuloInstanceID = new String(curator.getData().forPath(ZookeeperPath.CONFIG_ACCUMULO_INSTANCE_ID), "UTF-8");
    fluoInstanceID = new String(curator.getData().forPath(ZookeeperPath.CONFIG_FLUO_INSTANCE_ID), "UTF-8");

    table = new String(curator.getData().forPath(ZookeeperPath.CONFIG_ACCUMULO_TABLE), "UTF-8");

    ByteArrayInputStream bais = new ByteArrayInputStream(curator.getData().forPath(ZookeeperPath.CONFIG_FLUO_OBSERVERS));
    DataInputStream dis = new DataInputStream(bais);
    
    observers = Collections.unmodifiableMap(readObservers(dis));
    weakObservers = Collections.unmodifiableMap(readObservers(dis));
    allObserversColumns = new HashSet<>();
    allObserversColumns.addAll(observers.keySet());
    allObserversColumns.addAll(weakObservers.keySet());
    allObserversColumns = Collections.unmodifiableSet(allObserversColumns);

    bais = new ByteArrayInputStream(curator.getData().forPath(ZookeeperPath.CONFIG_SHARED));
    Properties sharedProps = new Properties(); 
    sharedProps.load(bais);
    config.addConfiguration(ConfigurationConverter.getConfiguration(sharedProps));
  }

  private static Map<Column,ObserverConfiguration> readObservers(DataInputStream dis) throws IOException {

    HashMap<Column,ObserverConfiguration> omap = new HashMap<>();
    
    int num = WritableUtils.readVInt(dis);
    for (int i = 0; i < num; i++) {
      Column col = new Column();
      col.readFields(dis);
      String clazz = dis.readUTF();
      Map<String,String> params = new HashMap<>();
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
      this.resources = new SharedResources(this);
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
    
  public SharedResources getSharedResources() {
    return resources;
  }
    
  public FluoConfiguration getConfiguration() {
    return config;
  }

  public long getRollbackTime() {
    return rollbackTime;
  }
  
  public int getOraclePort() {
    return oraclePort;
  }

  @Override
  public void close() {
    resources.close();
  }
}
