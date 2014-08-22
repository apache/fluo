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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import io.fluo.api.client.FluoAdmin;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Column;
import io.fluo.api.observer.Observer;
import io.fluo.api.observer.Observer.NotificationType;
import io.fluo.api.observer.Observer.ObservedColumn;
import io.fluo.core.util.CuratorUtil;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fluo Admin Implementation
 */
public class FluoAdminImpl implements FluoAdmin {

  private static Logger logger = LoggerFactory.getLogger(FluoAdminImpl.class);
  private FluoConfiguration config;
  
  public FluoAdminImpl(FluoConfiguration config) {
    this.config = config;
    if (!config.hasRequiredAdminProps()) {
      throw new IllegalArgumentException("Admin configuration is missing required properties");
    }
  }
  
  public void initialize() throws AlreadyInitializedException {
    try {
      Connector conn = new ZooKeeperInstance(config.getAccumuloInstance(), config.getZookeepers())
          .getConnector(config.getAccumuloUser(),new PasswordToken(config.getAccumuloPassword()));

      if (config.getClearZookeeper()) {
        try (CuratorFramework curator = CuratorUtil.getCurator(config.getZookeepers(), config.getZookeeperTimeout())) {
          curator.start();
          String zkRoot = config.getZookeeperRoot();
          try {
            curator.delete().deletingChildrenIfNeeded().forPath(zkRoot);
          } catch(KeeperException.NoNodeException nne) {
          } catch(Exception e) {
            logger.error("An error occurred deleting Zookeeper node. node=[" + zkRoot + "], error=[" + e.getMessage() + "]");
            throw new RuntimeException(e);
          }
        }
      }

      Operations.initialize(conn, config.getZookeeperRoot(), config.getAccumuloTable());

      updateSharedConfig();
      
      // TODO add fluo version to context name to make it unique
      String contextName = "fluo";
      conn.instanceOperations().setProperty(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "fluo", config.getAccumuloClasspath());
      conn.tableOperations().setProperty(config.getAccumuloTable(), Property.TABLE_CLASSPATH.getKey(), contextName);

      conn.tableOperations().setProperty(config.getAccumuloTable(), Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");
    } catch (NodeExistsException nee) {
      throw new AlreadyInitializedException(nee);
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new RuntimeException(e);
    }
  }

  public void updateSharedConfig() {
    
    try {
      Connector conn = new ZooKeeperInstance(config.getAccumuloInstance(), config.getZookeepers())
           .getConnector(config.getAccumuloUser(),new PasswordToken(config.getAccumuloPassword()));

      Properties sharedProps = new Properties();

      Map<Column,ObserverConfiguration> colObservers = new HashMap<Column,ObserverConfiguration>();
      Map<Column,ObserverConfiguration> weakObservers = new HashMap<Column,ObserverConfiguration>();

      Iterator<String> iter = config.getKeys();
      while (iter.hasNext()) {
        String key = iter.next();
        if (key.startsWith(FluoConfiguration.OBSERVER_PREFIX)) {
          addObserver(colObservers, weakObservers, config.getString(key));
        } else if (key.equals(FluoConfiguration.TRANSACTION_ROLLBACK_TIME_PROP)) {
          sharedProps.setProperty(key, Long.toString(config.getLong(key)));
        }
      }
      Operations.updateObservers(conn, config.getZookeeperRoot(), colObservers, weakObservers);
      Operations.updateSharedConfig(conn, config.getZookeeperRoot(), sharedProps);
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new RuntimeException(e);
    }
  }

  private static void addObserver(Map<Column,ObserverConfiguration> observers, 
      Map<Column,ObserverConfiguration> weakObservers, String value) throws Exception {
    
    String[] fields = value.split(",");

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
