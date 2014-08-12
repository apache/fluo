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
package io.fluo.core.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import io.fluo.api.client.FluoAdmin;
import io.fluo.api.config.ConnectionProperties;
import io.fluo.api.config.InitializationProperties;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.config.TransactionConfiguration;
import io.fluo.api.config.WorkerProperties;
import io.fluo.api.data.Column;
import io.fluo.api.observer.Observer;
import io.fluo.api.observer.Observer.NotificationType;
import io.fluo.api.observer.Observer.ObservedColumn;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.zookeeper.ZooUtil;
import org.apache.accumulo.fate.zookeeper.ZooUtil.NodeMissingPolicy;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Fluo Admin Implementation
 */
public class FluoAdminImpl implements FluoAdmin {
  
  /**
   * Initialize a Fluo instance
   * 
   * @param props see {@link io.fluo.api.config.InitializationProperties}
   */
  public void initialize(Properties props) throws AlreadyInitializedException {
    try {
      Connector conn = new ZooKeeperInstance(props.getProperty(ConnectionProperties.ACCUMULO_INSTANCE_PROP),
          props.getProperty(ConnectionProperties.ZOOKEEPER_CONNECT_PROP)).getConnector(props.getProperty(ConnectionProperties.ACCUMULO_USER_PROP),
          new PasswordToken(props.getProperty(ConnectionProperties.ACCUMULO_PASSWORD_PROP)));

      if (Boolean.valueOf(props.getProperty(InitializationProperties.CLEAR_ZOOKEEPER_PROP, "false"))) {
        ZooKeeper zk = new ZooKeeper(props.getProperty(ConnectionProperties.ZOOKEEPER_CONNECT_PROP), 30000, null);
        ZooUtil.recursiveDelete(zk, props.getProperty(ConnectionProperties.ZOOKEEPER_ROOT_PROP), NodeMissingPolicy.SKIP);
        zk.close();
      }

      Operations.initialize(conn, props.getProperty(ConnectionProperties.ZOOKEEPER_ROOT_PROP), props.getProperty(InitializationProperties.TABLE_PROP));

      updateWorkerConfig(props);

      if (props.getProperty(InitializationProperties.CLASSPATH_PROP) != null) {
        // TODO add fluo version to context name to make it unique
        String contextName = "fluo";
        conn.instanceOperations().setProperty(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "fluo",
            props.getProperty(InitializationProperties.CLASSPATH_PROP));
        conn.tableOperations().setProperty(props.getProperty(InitializationProperties.TABLE_PROP), Property.TABLE_CLASSPATH.getKey(), contextName);
      }

      conn.tableOperations().setProperty(props.getProperty(InitializationProperties.TABLE_PROP), Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");
    } catch (NodeExistsException nee) {
      throw new AlreadyInitializedException(nee);
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new RuntimeException(e);
    }
  }

  /**
   * Update Worker configuration
   * 
   * @param props see {@link WorkerProperties}
   */
  public void updateWorkerConfig(Properties props) {
    try {
      Connector conn = new ZooKeeperInstance(props.getProperty(ConnectionProperties.ACCUMULO_INSTANCE_PROP),
          props.getProperty(ConnectionProperties.ZOOKEEPER_CONNECT_PROP)).getConnector(props.getProperty(ConnectionProperties.ACCUMULO_USER_PROP),
          new PasswordToken(props.getProperty(ConnectionProperties.ACCUMULO_PASSWORD_PROP)));

      Properties workerConfig = new Properties();

      Map<Column,ObserverConfiguration> colObservers = new HashMap<Column,ObserverConfiguration>();
      Map<Column,ObserverConfiguration> weakObservers = new HashMap<Column,ObserverConfiguration>();

      Set<Entry<Object,Object>> entries = props.entrySet();
      for (Entry<Object,Object> entry : entries) {
        String key = (String) entry.getKey();
        if (key.startsWith(WorkerProperties.OBSERVER_PREFIX_PROP)) {
          addObserver(colObservers, weakObservers, entry);
        } else if (key.startsWith(WorkerProperties.WORKER_PREFIX) || key.startsWith(TransactionConfiguration.TRANSACTION_PREFIX)) {
          workerConfig.setProperty((String) entry.getKey(), (String) entry.getValue());
        }
      }

      Operations.updateObservers(conn, props.getProperty(ConnectionProperties.ZOOKEEPER_ROOT_PROP), colObservers, weakObservers);
      Operations.updateWorkerConfig(conn, props.getProperty(ConnectionProperties.ZOOKEEPER_ROOT_PROP), workerConfig);
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new RuntimeException(e);
    }
  }

  private static void addObserver(Map<Column,ObserverConfiguration> observers, Map<Column,ObserverConfiguration> weakObservers, Entry<Object,Object> entry)
      throws Exception {
    String val = (String) entry.getValue();
    String[] fields = val.split(",");

    ObserverConfiguration observerConfig = new ObserverConfiguration(fields[0]);

    Map<String,String> params = new HashMap<String,String>();
    for (int i = 1; i < fields.length; i++) {
      String[] kv = fields[i].split("=");
      params.put(kv[0], kv[1]);
    }
    observerConfig.setParameters(params);

    Observer observer = Class.forName(observerConfig.getClassName()).asSubclass(Observer.class).newInstance();
    observer.init(observerConfig.getParameters());
    ObservedColumn observedCol = observer.getObservedColumn();

    if (observedCol.getType() == NotificationType.STRONG)
      observers.put(observedCol.getColumn(), observerConfig);
    else
      weakObservers.put(observedCol.getColumn(), observerConfig);
  }
}
