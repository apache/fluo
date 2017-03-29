/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.core.client;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.accumulo.iterators.GarbageCollectionIterator;
import org.apache.fluo.accumulo.iterators.NotificationIterator;
import org.apache.fluo.accumulo.util.AccumuloProps;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.accumulo.util.ZookeeperUtil;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.core.observer.ObserverUtil;
import org.apache.fluo.core.util.AccumuloUtil;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.hadoop.io.Text;
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
  private final CuratorFramework rootCurator;
  private CuratorFramework appCurator = null;

  private final String appRootDir;

  public FluoAdminImpl(FluoConfiguration config) {
    this.config = config;
    if (!config.hasRequiredAdminProps()) {
      throw new IllegalArgumentException("Admin configuration is missing required properties");
    }

    appRootDir = ZookeeperUtil.parseRoot(config.getAppZookeepers());
    rootCurator = CuratorUtil.newRootFluoCurator(config);
    rootCurator.start();
  }

  private synchronized CuratorFramework getAppCurator() {
    if (appCurator == null) {
      appCurator = CuratorUtil.newAppCurator(config);
      appCurator.start();
    }
    return appCurator;
  }

  @Override
  public void initialize(InitializationOptions opts) throws AlreadyInitializedException,
      TableExistsException {
    Preconditions.checkArgument(!ZookeeperUtil.parseRoot(config.getInstanceZookeepers())
        .equals("/"), "The Zookeeper connection string (set by 'fluo.client.zookeeper.connect') "
        + " must have a chroot suffix.");

    if (zookeeperInitialized() && !opts.getClearZookeeper()) {
      throw new AlreadyInitializedException("Fluo application already initialized at "
          + config.getAppZookeepers());
    }

    Connector conn = AccumuloUtil.getConnector(config);

    boolean tableExists = conn.tableOperations().exists(config.getAccumuloTable());
    if (tableExists && !opts.getClearTable()) {
      throw new TableExistsException("Accumulo table already exists " + config.getAccumuloTable());
    }

    // With preconditions met, it's now OK to delete table & zookeeper root (if they exist)

    if (tableExists) {
      logger.info("The Accumulo table '{}' will be dropped and created as requested by user",
          config.getAccumuloTable());
      try {
        conn.tableOperations().delete(config.getAccumuloTable());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    try {
      if (rootCurator.checkExists().forPath(appRootDir) != null) {
        logger.info("Clearing Fluo '{}' application in Zookeeper at {}",
            config.getApplicationName(), config.getAppZookeepers());
        rootCurator.delete().deletingChildrenIfNeeded().forPath(appRootDir);
      }
    } catch (KeeperException.NoNodeException nne) {
      // it's ok if node doesn't exist
    } catch (Exception e) {
      logger.error("An error occurred deleting Zookeeper root of [" + config.getAppZookeepers()
          + "], error=[" + e.getMessage() + "]");
      throw new RuntimeException(e);
    }

    try {
      initialize(conn);
      updateSharedConfig();

      if (!config.getAccumuloClasspath().trim().isEmpty()) {
        // TODO add fluo version to context name to make it unique
        String contextName = "fluo";
        conn.instanceOperations().setProperty(
            AccumuloProps.VFS_CONTEXT_CLASSPATH_PROPERTY + "fluo", config.getAccumuloClasspath());
        conn.tableOperations().setProperty(config.getAccumuloTable(),
            AccumuloProps.TABLE_CLASSPATH, contextName);
      }

      conn.tableOperations().setProperty(config.getAccumuloTable(),
          AccumuloProps.TABLE_BLOCKCACHE_ENABLED, "true");
    } catch (NodeExistsException nee) {
      throw new AlreadyInitializedException();
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  private void initialize(Connector conn) throws Exception {

    final String accumuloInstanceName = conn.getInstance().getInstanceName();
    final String accumuloInstanceID = conn.getInstance().getInstanceID();
    final String fluoApplicationID = UUID.randomUUID().toString();

    // Create node specified by chroot suffix of Zookeeper connection string (if it doesn't exist)
    CuratorUtil.putData(rootCurator, appRootDir, new byte[0], CuratorUtil.NodeExistsPolicy.FAIL);

    // Retrieve Fluo curator now that chroot has been created
    CuratorFramework curator = getAppCurator();

    // Initialize Zookeeper & Accumulo for this Fluo instance
    // TODO set Fluo data version
    CuratorUtil.putData(curator, ZookeeperPath.CONFIG, new byte[0],
        CuratorUtil.NodeExistsPolicy.FAIL);
    CuratorUtil.putData(curator, ZookeeperPath.CONFIG_ACCUMULO_TABLE, config.getAccumuloTable()
        .getBytes(StandardCharsets.UTF_8), CuratorUtil.NodeExistsPolicy.FAIL);
    CuratorUtil.putData(curator, ZookeeperPath.CONFIG_ACCUMULO_INSTANCE_NAME,
        accumuloInstanceName.getBytes(StandardCharsets.UTF_8), CuratorUtil.NodeExistsPolicy.FAIL);
    CuratorUtil.putData(curator, ZookeeperPath.CONFIG_ACCUMULO_INSTANCE_ID,
        accumuloInstanceID.getBytes(StandardCharsets.UTF_8), CuratorUtil.NodeExistsPolicy.FAIL);
    CuratorUtil.putData(curator, ZookeeperPath.CONFIG_FLUO_APPLICATION_ID,
        fluoApplicationID.getBytes(StandardCharsets.UTF_8), CuratorUtil.NodeExistsPolicy.FAIL);
    CuratorUtil.putData(curator, ZookeeperPath.ORACLE_SERVER, new byte[0],
        CuratorUtil.NodeExistsPolicy.FAIL);
    CuratorUtil.putData(curator, ZookeeperPath.ORACLE_MAX_TIMESTAMP, new byte[] {'2'},
        CuratorUtil.NodeExistsPolicy.FAIL);
    CuratorUtil.putData(curator, ZookeeperPath.ORACLE_GC_TIMESTAMP, new byte[] {'0'},
        CuratorUtil.NodeExistsPolicy.FAIL);

    conn.tableOperations().create(config.getAccumuloTable(), false);
    Map<String, Set<Text>> groups = new HashMap<>();
    groups.put("notify", Collections.singleton(ByteUtil.toText(ColumnConstants.NOTIFY_CF)));
    conn.tableOperations().setLocalityGroups(config.getAccumuloTable(), groups);

    IteratorSetting gcIter = new IteratorSetting(10, "gc", GarbageCollectionIterator.class);
    GarbageCollectionIterator.setZookeepers(gcIter, config.getAppZookeepers());

    conn.tableOperations().attachIterator(config.getAccumuloTable(), gcIter,
        EnumSet.of(IteratorUtil.IteratorScope.majc, IteratorUtil.IteratorScope.minc));

    // the order relative to gc iter should not matter
    IteratorSetting ntfyIter = new IteratorSetting(11, "ntfy", NotificationIterator.class);

    conn.tableOperations().attachIterator(config.getAccumuloTable(), ntfyIter,
        EnumSet.of(IteratorUtil.IteratorScope.majc, IteratorUtil.IteratorScope.minc));
  }

  @Override
  public void updateSharedConfig() {

    Properties sharedProps = new Properties();
    Iterator<String> iter = config.getKeys();
    while (iter.hasNext()) {
      String key = iter.next();
      if (key.equals(FluoConfiguration.TRANSACTION_ROLLBACK_TIME_PROP)) {
        sharedProps.setProperty(key, Long.toString(config.getLong(key)));
      } else if (key.startsWith(FluoConfiguration.APP_PREFIX)) {
        sharedProps.setProperty(key, config.getRawString(key));
      }
    }

    try {
      CuratorFramework curator = getAppCurator();
      ObserverUtil.initialize(curator, config);

      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      sharedProps.store(baos, "Shared java props");

      CuratorUtil.putData(curator, ZookeeperPath.CONFIG_SHARED, baos.toByteArray(),
          CuratorUtil.NodeExistsPolicy.OVERWRITE);
    } catch (Exception e) {
      throw new FluoException("Failed to update shared configuration in Zookeeper", e);
    }
  }

  @Override
  public void close() {
    rootCurator.close();
    if (appCurator != null) {
      appCurator.close();
    }
  }

  public boolean oracleExists() {
    CuratorFramework curator = getAppCurator();
    try {
      return curator.checkExists().forPath(ZookeeperPath.ORACLE_SERVER) != null
          && !curator.getChildren().forPath(ZookeeperPath.ORACLE_SERVER).isEmpty();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean zookeeperInitialized() {
    try {
      return rootCurator.checkExists().forPath(appRootDir) != null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean accumuloTableExists() {
    Connector conn = AccumuloUtil.getConnector(config);
    return conn.tableOperations().exists(config.getAccumuloTable());
  }
}
