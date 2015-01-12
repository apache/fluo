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

import com.google.common.base.Preconditions;
import io.fluo.accumulo.util.ZookeeperPath;
import io.fluo.accumulo.util.ZookeeperUtil;
import io.fluo.api.client.FluoAdmin;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Column;
import io.fluo.api.observer.Observer;
import io.fluo.api.observer.Observer.NotificationType;
import io.fluo.api.observer.Observer.ObservedColumn;
import io.fluo.core.util.AccumuloUtil;
import io.fluo.core.util.CuratorUtil;
import io.fluo.core.worker.ObserverContext;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.conf.Property;
import org.apache.commons.configuration.ConfigurationUtils;
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
  private final FluoConfiguration config;
  
  public FluoAdminImpl(FluoConfiguration config) {
    this.config = config;
    if (!config.hasRequiredAdminProps()) {
      throw new IllegalArgumentException("Admin configuration is missing required properties");
    }
  }
  
  @Override
  public void initialize(InitOpts opts) throws AlreadyInitializedException, TableExistsException {
    Preconditions.checkArgument(ZookeeperUtil.parseRoot(config.getZookeepers()).equals("/") == false, 
        "The Zookeeper connection string (set by 'io.fluo.client.zookeeper.connect') must have a chroot suffix.");

    if (zookeeperInitialized(config) && !opts.getClearZookeeper()) {
      throw new AlreadyInitializedException("Fluo instance already initialized at " + config.getZookeepers());
    }

    Connector conn = AccumuloUtil.getConnector(config);
    boolean tableExists = conn.tableOperations().exists(config.getAccumuloTable());
    if (tableExists && !opts.getClearTable()) {
      throw new TableExistsException("Accumulo table already exists " + config.getAccumuloTable());
    }

    // With preconditions met, it's now OK to delete table & zookeeper root (if they exist)

    if (tableExists) {
      logger.info("Accumulo table {} will be dropped and created as requested by user", config.getAccumuloTable());
      try {
        conn.tableOperations().delete(config.getAccumuloTable());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    try (CuratorFramework curator = CuratorUtil.newRootFluoCurator(config)) {
      curator.start();
      try {
        String zkRoot = ZookeeperUtil.parseRoot(config.getZookeepers());
        if (curator.checkExists().forPath(zkRoot) != null) { 
          logger.info("Clearing Fluo instance in Zookeeper at {}", config.getZookeepers());
          curator.delete().deletingChildrenIfNeeded().forPath(zkRoot);
        }
      } catch(KeeperException.NoNodeException nne) {
      } catch(Exception e) {
        logger.error("An error occurred deleting Zookeeper root of [" + config.getZookeepers() + "], error=[" + e.getMessage() + "]");
        throw new RuntimeException(e);
      }
    }

    try {
      Operations.initialize(config, conn);
      updateSharedConfig();

      if (!config.getAccumuloClasspath().trim().isEmpty()) {
        // TODO add fluo version to context name to make it unique
        String contextName = "fluo";
        conn.instanceOperations().setProperty(Property.VFS_CONTEXT_CLASSPATH_PROPERTY.getKey() + "fluo", config.getAccumuloClasspath());
        conn.tableOperations().setProperty(config.getAccumuloTable(), Property.TABLE_CLASSPATH.getKey(), contextName);
      }

      conn.tableOperations().setProperty(config.getAccumuloTable(), Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");
    } catch (NodeExistsException nee) {
      throw new AlreadyInitializedException();
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new RuntimeException(e);
    }
  }

  @Override
  public void updateSharedConfig() {
    
    try {
      logger.info("Setting up observers using app config: {}", ConfigurationUtils.toString(config.subset(FluoConfiguration.APP_PREFIX)));
      
      Map<Column,ObserverConfiguration> colObservers = new HashMap<>();
      Map<Column,ObserverConfiguration> weakObservers = new HashMap<>();
      for (ObserverConfiguration observerConfig : config.getObserverConfig()) {
        
        Observer observer = Class.forName(observerConfig.getClassName()).asSubclass(Observer.class).newInstance();
        
        logger.info("Setting up observer {} using params {}.", observer.getClass().getSimpleName(), observerConfig.getParameters());
        observer.init(new ObserverContext(config.subset(FluoConfiguration.APP_PREFIX), observerConfig.getParameters()));
        
        ObservedColumn observedCol = observer.getObservedColumn();
        if (observedCol.getType() == NotificationType.STRONG)
          colObservers.put(observedCol.getColumn(), observerConfig);
        else
          weakObservers.put(observedCol.getColumn(), observerConfig);
      }

      Properties sharedProps = new Properties();
      Iterator<String> iter = config.getKeys();
      while (iter.hasNext()) {
        String key = iter.next();
        if (key.equals(FluoConfiguration.TRANSACTION_ROLLBACK_TIME_PROP)) {
          sharedProps.setProperty(key, Long.toString(config.getLong(key)));
        } else if (key.startsWith(FluoConfiguration.APP_PREFIX)){
          sharedProps.setProperty(key, config.getProperty(key).toString());
        }
      }
      
      Operations.updateObservers(config, colObservers, weakObservers);
      Operations.updateSharedConfig(config, sharedProps);
      
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      throw new RuntimeException(e);
    }
  }
  
  public static boolean oracleExists(FluoConfiguration config) {
    try (CuratorFramework curator = CuratorUtil.newFluoCurator(config)) {
      curator.start();
      try {
        if (curator.checkExists().forPath(ZookeeperPath.ORACLE_SERVER) == null) {
          return false;
        } else {
          return !curator.getChildren().forPath(ZookeeperPath.ORACLE_SERVER).isEmpty();
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } 
  }
  
  public static boolean zookeeperInitialized(FluoConfiguration config) {
    try (CuratorFramework curator = CuratorUtil.newRootFluoCurator(config)) {
      curator.start();
      String zkRoot = ZookeeperUtil.parseRoot(config.getZookeepers());
      try {
        return curator.checkExists().forPath(zkRoot) != null;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } 
  }
  
  public static boolean accumuloTableExists(FluoConfiguration config) {
    Connector conn = AccumuloUtil.getConnector(config);
    return conn.tableOperations().exists(config.getAccumuloTable());
  }
}
