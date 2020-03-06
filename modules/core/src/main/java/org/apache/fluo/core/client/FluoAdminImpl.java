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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.fluo.accumulo.iterators.GarbageCollectionIterator;
import org.apache.fluo.accumulo.iterators.NotificationIterator;
import org.apache.fluo.accumulo.summarizer.FluoSummarizer;
import org.apache.fluo.accumulo.util.AccumuloProps;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.accumulo.util.ZookeeperUtil;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.observer.ObserverUtil;
import org.apache.fluo.core.util.AccumuloUtil;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.fluo.core.util.OracleServerUtils;
import org.apache.fluo.core.worker.finder.hash.PartitionManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
  public void initialize(InitializationOptions opts)
      throws AlreadyInitializedException, TableExistsException {
    if (!config.hasRequiredAdminProps()) {
      throw new IllegalArgumentException("Admin configuration is missing required properties");
    }
    Preconditions.checkArgument(
        !ZookeeperUtil.parseRoot(config.getInstanceZookeepers()).equals("/"),
        "The Zookeeper connection string (set by 'fluo.connection.zookeepers') "
            + " must have a chroot suffix.");

    Preconditions.checkArgument(
        config.getObserverJarsUrl().isEmpty() || config.getObserverInitDir().isEmpty(),
        "Only one of 'fluo.observer.init.dir' and 'fluo.observer.jars.url' can be set");
    if (applicationRunning()) {
      throw new AlreadyInitializedException("Error - The Fluo '" + config.getApplicationName()
          + "' application" + " is already running and must be stopped before initializing. "
          + " Aborted initialization.");
    }
    if (zookeeperInitialized() && !opts.getClearZookeeper()) {
      throw new AlreadyInitializedException(
          "Fluo application already initialized at " + config.getAppZookeepers());
    }

    try (AccumuloClient client = AccumuloUtil.getClient(config)) {
      initialize(opts, client);
    }
  }

  private void initialize(InitializationOptions opts, AccumuloClient client)
      throws TableExistsException, AlreadyInitializedException {
    boolean tableExists = client.tableOperations().exists(config.getAccumuloTable());
    if (tableExists && !opts.getClearTable()) {
      throw new TableExistsException("Accumulo table already exists " + config.getAccumuloTable());
    }

    // With preconditions met, it's now OK to delete table & zookeeper root (if they exist)

    if (tableExists) {
      logger.info("The Accumulo table '{}' will be dropped and created as requested by user",
          config.getAccumuloTable());
      try {
        client.tableOperations().delete(config.getAccumuloTable());
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
      initializeApplicationInZooKeeper(client);

      String accumuloJars;
      if (!config.getAccumuloJars().trim().isEmpty()) {
        if (config.getDfsRoot().trim().isEmpty()) {
          throw new IllegalStateException("The property " + FluoConfiguration.ACCUMULO_JARS_PROP
              + " is set and " + FluoConfiguration.DFS_ROOT_PROP
              + " is not set.  So there is nowhere to copy the jars.");
        }
        accumuloJars = config.getAccumuloJars().trim();
      } else if (!config.getDfsRoot().trim().isEmpty()) {
        accumuloJars = getJarsFromClasspath();
      } else {
        accumuloJars = "";
      }

      String accumuloClasspath = "";
      if (!accumuloJars.isEmpty()) {
        accumuloClasspath = copyJarsToDfs(accumuloJars, "lib/accumulo");
      }

      Map<String, String> ntcProps = new HashMap<>();

      if (!accumuloClasspath.isEmpty()) {
        String contextName = "fluo-" + config.getApplicationName();
        client.instanceOperations().setProperty(
            AccumuloProps.VFS_CONTEXT_CLASSPATH_PROPERTY + contextName, accumuloClasspath);
        ntcProps.put(AccumuloProps.TABLE_CLASSPATH, contextName);
      }

      if (config.getObserverJarsUrl().isEmpty() && !config.getObserverInitDir().trim().isEmpty()) {
        String observerUrl = copyDirToDfs(config.getObserverInitDir().trim(), "lib/observers");
        config.setObserverJarsUrl(observerUrl);
      }

      ntcProps.put(AccumuloProps.TABLE_BLOCKCACHE_ENABLED, "true");
      ntcProps.put(AccumuloProps.TABLE_DELETE_BEHAVIOR, AccumuloProps.TABLE_DELETE_BEHAVIOR_VALUE);

      NewTableConfiguration ntc = new NewTableConfiguration().withoutDefaultIterators();

      ntc.setLocalityGroups(Collections.singletonMap(ColumnConstants.NOTIFY_LOCALITY_GROUP_NAME,
          Collections.singleton(new Text(ColumnConstants.NOTIFY_CF.toArray()))));

      ntc.enableSummarization(FluoSummarizer.CONFIG);

      configureIterators(ntc);

      ntc.setProperties(ntcProps);
      client.tableOperations().create(config.getAccumuloTable(), ntc);

      updateSharedConfig();
    } catch (NodeExistsException nee) {
      throw new AlreadyInitializedException();
    } catch (Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  private void configureIterators(NewTableConfiguration ntc) {
    IteratorSetting gcIter =
        new IteratorSetting(10, ColumnConstants.GC_CF.toString(), GarbageCollectionIterator.class);
    GarbageCollectionIterator.setZookeepers(gcIter, config.getAppZookeepers());
    // the order relative to gc iter should not matter
    IteratorSetting ntfyIter =
        new IteratorSetting(11, ColumnConstants.NOTIFY_CF.toString(), NotificationIterator.class);

    EnumSet<IteratorScope> scopes =
        EnumSet.of(IteratorUtil.IteratorScope.majc, IteratorUtil.IteratorScope.minc);
    ntc.attachIterator(gcIter, scopes);
    ntc.attachIterator(ntfyIter, scopes);
  }

  @Override
  public void remove() {
    if (applicationRunning()) {
      throw new FluoException("Error - The Fluo '" + config.getApplicationName() + "' application"
          + " is already running and must be stopped before removing. Aborted remove.");
    }
    if (!config.hasRequiredAdminProps()) {
      throw new IllegalArgumentException("Admin configuration is missing required properties");
    }
    Preconditions.checkArgument(
        !ZookeeperUtil.parseRoot(config.getInstanceZookeepers()).equals("/"),
        "The Zookeeper connection string (set by 'fluo.connection.zookeepers') "
            + " must have a chroot suffix.");

    if (OracleServerUtils.oracleExists(getAppCurator())) {
      throw new FluoException("Must stop the oracle server to remove an application");
    }

    try (AccumuloClient client = AccumuloUtil.getClient(config)) {
      boolean tableExists = client.tableOperations().exists(config.getAccumuloTable());
      // With preconditions met, it's now OK to delete table & zookeeper root (if they exist)
      if (tableExists) {
        logger.info("The Accumulo table '{}' will be dropped", config.getAccumuloTable());
        try {
          client.tableOperations().delete(config.getAccumuloTable());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
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
  }

  private void initializeApplicationInZooKeeper(AccumuloClient client) throws Exception {

    final String accumuloInstanceName =
        client.properties().getProperty(AccumuloProps.CLIENT_INSTANCE_NAME);
    final String accumuloInstanceID = client.instanceOperations().getInstanceID();
    final String fluoApplicationID = UUID.randomUUID().toString();

    // Create node specified by chroot suffix of Zookeeper connection string (if it doesn't exist)
    CuratorUtil.putData(rootCurator, appRootDir, new byte[0], CuratorUtil.NodeExistsPolicy.FAIL);

    // Retrieve Fluo curator now that chroot has been created
    CuratorFramework curator = getAppCurator();

    // Initialize Zookeeper & Accumulo for this Fluo instance
    // TODO set Fluo data version
    CuratorUtil.putData(curator, ZookeeperPath.CONFIG, new byte[0],
        CuratorUtil.NodeExistsPolicy.FAIL);
    CuratorUtil.putData(curator, ZookeeperPath.CONFIG_ACCUMULO_TABLE,
        config.getAccumuloTable().getBytes(StandardCharsets.UTF_8),
        CuratorUtil.NodeExistsPolicy.FAIL);
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
  }

  @Override
  public void updateSharedConfig() {
    if (!config.hasRequiredAdminProps()) {
      throw new IllegalArgumentException("Admin configuration is missing required properties");
    }
    if (applicationRunning()) {
      throw new FluoException("Error - The Fluo '" + config.getApplicationName() + "' application"
          + " is already running and must be stopped before updating shared configuration. "
          + " Aborted update.");
    }
    Properties sharedProps = new Properties();
    Iterator<String> iter = config.getKeys();
    while (iter.hasNext()) {
      String key = iter.next();
      if (!key.startsWith(FluoConfiguration.CONNECTION_PREFIX)) {
        sharedProps.setProperty(key, config.getRawString(key));
      }
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      CuratorFramework curator = getAppCurator();
      ObserverUtil.initialize(curator, config);

      sharedProps.store(baos, "Shared java props");

      CuratorUtil.putData(curator, ZookeeperPath.CONFIG_SHARED, baos.toByteArray(),
          CuratorUtil.NodeExistsPolicy.OVERWRITE);
    } catch (Exception e) {
      throw new FluoException("Failed to update shared configuration in Zookeeper", e);
    }
  }

  @Override
  public SimpleConfiguration getConnectionConfig() {
    return new SimpleConfiguration(config);
  }

  @Override
  public SimpleConfiguration getApplicationConfig() {
    return getZookeeperConfig(config);
  }

  private String copyDirToDfs(String srcDir, String destDir) {
    return copyDirToDfs(config.getDfsRoot(), config.getApplicationName(), srcDir, destDir);
  }

  @VisibleForTesting
  public static String copyDirToDfs(String dfsRoot, String appName, String srcDir, String destDir) {
    String dfsAppRoot = dfsRoot + "/" + appName;
    String dfsDestDir = dfsAppRoot + "/" + destDir;

    try (FileSystem fs = FileSystem.get(new URI(dfsRoot), new Configuration())) {
      fs.delete(new Path(dfsDestDir), true);
      fs.mkdirs(new Path(dfsAppRoot));
      fs.copyFromLocalFile(new Path(srcDir), new Path(dfsDestDir));
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return dfsDestDir;
  }

  private String copyJarsToDfs(String jars, String destDir) {
    String dfsAppRoot = config.getDfsRoot() + "/" + config.getApplicationName();
    String dfsDestDir = dfsAppRoot + "/" + destDir;

    FileSystem fs = null;
    try {
      fs = FileSystem.get(new URI(config.getDfsRoot()), new Configuration());
      fs.mkdirs(new Path(dfsDestDir));
    } catch (Exception e) {
      logger.error("Failed to create DFS directory {}", dfsDestDir);
      if (fs != null) {
        try {
          fs.close();
        } catch (IOException ioe) {
          throw new IllegalStateException(ioe);
        }
      }
      throw new IllegalStateException(e);
    }

    StringBuilder classpath = new StringBuilder();
    for (String jarPath : jars.split(",")) {
      File jarFile = new File(jarPath);
      String jarName = jarFile.getName();
      try {
        fs.copyFromLocalFile(new Path(jarPath), new Path(dfsDestDir));
      } catch (IOException e) {
        logger.error("Failed to copy file {} to DFS directory {}", jarPath, dfsDestDir);
        try {
          fs.close();
        } catch (IOException ioe) {
          throw new IllegalStateException(ioe);
        }
        throw new IllegalStateException(e);
      }
      if (classpath.length() != 0) {
        classpath.append(",");
      }
      classpath.append(dfsDestDir).append("/").append(jarName);
    }

    try {
      fs.close();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return classpath.toString();
  }

  public static boolean isInitialized(FluoConfiguration config) {
    try (CuratorFramework rootCurator = CuratorUtil.newRootFluoCurator(config)) {
      rootCurator.start();
      String appRootDir = ZookeeperUtil.parseRoot(config.getAppZookeepers());
      return rootCurator.checkExists().forPath(appRootDir) != null;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static FluoConfiguration mergeZookeeperConfig(FluoConfiguration config) {
    SimpleConfiguration zooConfig = getZookeeperConfig(config);
    FluoConfiguration copy = new FluoConfiguration(config);
    for (Map.Entry<String, String> entry : zooConfig.toMap().entrySet()) {
      copy.setProperty(entry.getKey(), entry.getValue());
    }
    return copy;
  }

  public static SimpleConfiguration getZookeeperConfig(FluoConfiguration config) {
    if (!isInitialized(config)) {
      throw new IllegalStateException(
          "Fluo Application '" + config.getApplicationName() + "' has not been initialized");
    }

    SimpleConfiguration zooConfig = new SimpleConfiguration();

    try (CuratorFramework curator = CuratorUtil.newAppCurator(config)) {
      curator.start();

      try (ByteArrayInputStream bais =
          new ByteArrayInputStream(curator.getData().forPath(ZookeeperPath.CONFIG_SHARED))) {

        Properties sharedProps = new Properties();
        sharedProps.load(bais);
        for (String prop : sharedProps.stringPropertyNames()) {
          zooConfig.setProperty(prop, sharedProps.getProperty(prop));
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
    return zooConfig;
  }

  @Override
  public void close() {
    rootCurator.close();
    if (appCurator != null) {
      appCurator.close();
    }
  }

  private String getJarsFromClasspath() {

    String sep = System.getProperty("path.separator");
    String[] paths = System.getProperty("java.class.path").split("[" + sep + "]");

    String regex = config.getString(FluoConfigurationImpl.ACCUMULO_JARS_REGEX_PROP,
        FluoConfigurationImpl.ACCUMULO_JARS_REGEX_DEFAULT);
    Pattern pattern = Pattern.compile(regex);

    StringBuilder jars = new StringBuilder();
    for (String path : paths) {
      java.nio.file.Path name = Paths.get(path).getFileName();
      if (name != null) {
        String jarName = name.toString();
        if (pattern.matcher(jarName).matches()) {
          if (jars.length() != 0) {
            jars.append(",");
          }
          jars.append(path);
        }
      }
    }

    logger.debug("Found Fluo Accumulo jars {} ", jars);

    return jars.toString();
  }

  public static boolean oracleExists(CuratorFramework curator) {
    return numOracles(curator) > 0;
  }

  public boolean oracleExists() {
    return oracleExists(getAppCurator());
  }

  private static int numOracles(CuratorFramework curator) {
    try {
      LeaderLatch leaderLatch = new LeaderLatch(curator, ZookeeperPath.ORACLE_SERVER);
      return leaderLatch.getParticipants().size();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public int numOracles() {
    return numOracles(getAppCurator());
  }

  public static int numWorkers(CuratorFramework curator) {
    int numWorkers = 0;
    try {
      if (curator.checkExists().forPath(ZookeeperPath.FINDERS) != null) {
        for (String path : curator.getChildren().forPath(ZookeeperPath.FINDERS)) {
          if (path.startsWith(PartitionManager.ZK_FINDER_PREFIX)) {
            numWorkers++;
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return numWorkers;
  }

  public int numWorkers() {
    return numWorkers(getAppCurator());
  }

  public static boolean applicationRunning(CuratorFramework curator) {
    return oracleExists(curator) || (numWorkers(curator) > 0);
  }

  public boolean applicationRunning() {
    return applicationRunning(getAppCurator());
  }

  public boolean zookeeperInitialized() {
    try {
      return rootCurator.checkExists().forPath(appRootDir) != null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean accumuloTableExists() {
    if (!config.hasRequiredAdminProps()) {
      throw new IllegalArgumentException("Admin configuration is missing required properties");
    }
    try (AccumuloClient client = AccumuloUtil.getClient(config)) {
      return client.tableOperations().exists(config.getAccumuloTable());
    }
  }
}
