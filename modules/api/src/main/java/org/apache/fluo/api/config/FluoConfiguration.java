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

package org.apache.fluo.api.config;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.observer.ObserverProvider;
import org.apache.fluo.api.observer.ObserverProvider.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration helper class for Fluo
 *
 * @since 1.0.0
 */
public class FluoConfiguration extends SimpleConfiguration {

  private static final long serialVersionUID = 1L;

  private static final Logger log = LoggerFactory.getLogger(FluoConfiguration.class);

  public static final String FLUO_PREFIX = "fluo";

  // Client properties
  private static final String CLIENT_PREFIX = FLUO_PREFIX + ".client";
  /**
   * @deprecated since 1.2.0 replaced by fluo.connection.application.name
   */
  @Deprecated
  public static final String CLIENT_APPLICATION_NAME_PROP = CLIENT_PREFIX + ".application.name";
  /**
   * @deprecated since 1.2.0 replaced by fluo.accumulo.password
   */
  @Deprecated
  public static final String CLIENT_ACCUMULO_PASSWORD_PROP = CLIENT_PREFIX + ".accumulo.password";
  /**
   * @deprecated since 1.2.0 replaced by fluo.accumulo.user
   */
  @Deprecated
  public static final String CLIENT_ACCUMULO_USER_PROP = CLIENT_PREFIX + ".accumulo.user";
  /**
   * @deprecated since 1.2.0 replaced by fluo.accumulo.instance
   */
  @Deprecated
  public static final String CLIENT_ACCUMULO_INSTANCE_PROP = CLIENT_PREFIX + ".accumulo.instance";
  /**
   * @deprecated since 1.2.0 replaced by fluo.accumulo.zookeepers
   */
  @Deprecated
  public static final String CLIENT_ACCUMULO_ZOOKEEPERS_PROP = CLIENT_PREFIX
      + ".accumulo.zookeepers";
  /**
   * @deprecated since 1.2.0 replaced by fluo.connection.zookeeper.timeout
   */
  @Deprecated
  public static final String CLIENT_ZOOKEEPER_TIMEOUT_PROP = CLIENT_PREFIX + ".zookeeper.timeout";
  /**
   * @deprecated since 1.2.0 replaced by fluo.connection.zookeepers
   */
  @Deprecated
  public static final String CLIENT_ZOOKEEPER_CONNECT_PROP = CLIENT_PREFIX + ".zookeeper.connect";
  /**
   * @deprecated since 1.2.0 replaced by fluo.connection.retry.timeout.ms
   */
  @Deprecated
  public static final String CLIENT_RETRY_TIMEOUT_MS_PROP = CLIENT_PREFIX + ".retry.timeout.ms";
  @Deprecated
  public static final int CLIENT_ZOOKEEPER_TIMEOUT_DEFAULT = 30000;
  @Deprecated
  public static final String CLIENT_ACCUMULO_ZOOKEEPERS_DEFAULT = "localhost";
  @Deprecated
  public static final String CLIENT_ZOOKEEPER_CONNECT_DEFAULT = "localhost/fluo";
  @Deprecated
  public static final int CLIENT_RETRY_TIMEOUT_MS_DEFAULT = -1;

  // Connection properties
  /**
   * @since 1.2.0
   */
  public static final String CONNECTION_PREFIX = FLUO_PREFIX + ".connection";
  /**
   * @since 1.2.0
   */
  public static final String CONNECTION_APPLICATION_NAME_PROP = CONNECTION_PREFIX
      + ".application.name";
  /**
   * @since 1.2.0
   */
  public static final String CONNECTION_ZOOKEEPER_TIMEOUT_PROP = CONNECTION_PREFIX
      + ".zookeeper.timeout";
  /**
   * @since 1.2.0
   */
  public static final String CONNECTION_ZOOKEEPERS_PROP = CONNECTION_PREFIX + ".zookeepers";
  /**
   * @since 1.2.0
   */
  public static final String CONNECTION_RETRY_TIMEOUT_MS_PROP = CONNECTION_PREFIX
      + ".retry.timeout.ms";
  public static final int CONNECTION_ZOOKEEPER_TIMEOUT_DEFAULT = 30000;
  public static final String CONNECTION_ZOOKEEPERS_DEFAULT = "localhost/fluo";
  public static final int CONNECTION_RETRY_TIMEOUT_MS_DEFAULT = -1;

  // Accumulo properties
  private static final String ACCUMULO_PREFIX = FLUO_PREFIX + ".accumulo";
  /**
   * @since 1.2.0
   */
  public static final String ACCUMULO_INSTANCE_PROP = ACCUMULO_PREFIX + ".instance";
  /**
   * @since 1.2.0
   */
  public static final String ACCUMULO_TABLE_PROP = ACCUMULO_PREFIX + ".table";
  /**
   * @since 1.2.0
   */
  public static final String ACCUMULO_PASSWORD_PROP = ACCUMULO_PREFIX + ".password";
  /**
   * @since 1.2.0
   */
  public static final String ACCUMULO_USER_PROP = ACCUMULO_PREFIX + ".user";
  /**
   * @since 1.2.0
   */
  public static final String ACCUMULO_ZOOKEEPERS_PROP = ACCUMULO_PREFIX + ".zookeepers";
  /**
   * @since 1.2.0
   */
  public static final String ACCUMULO_JARS_PROP = ACCUMULO_PREFIX + ".jars";
  // Accumulo defaults
  public static final String ACCUMULO_ZOOKEEPERS_DEFAULT = "localhost";
  public static final String ACCUMULO_JARS_DEFAULT = "";

  // DFS properties
  private static final String DFS_PREFIX = FLUO_PREFIX + ".dfs";
  /**
   * @since 1.2.0
   */
  public static final String DFS_ROOT_PROP = DFS_PREFIX + ".root";
  // DFS defaults
  public static final String DFS_ROOT_DEFAULT = "hdfs://localhost:8020/fluo";

  // Administration properties
  private static final String ADMIN_PREFIX = FLUO_PREFIX + ".admin";
  /**
   * @deprecated since 1.2.0 replaced by fluo.accumulo.table
   */
  @Deprecated
  public static final String ADMIN_ACCUMULO_TABLE_PROP = ADMIN_PREFIX + ".accumulo.table";
  /**
   * @deprecated since 1.2.0 replaced by fluo.observer.init.dir and fluo.observer.jars.url
   */
  @Deprecated
  public static final String ADMIN_ACCUMULO_CLASSPATH_PROP = ADMIN_PREFIX + ".accumulo.classpath";
  @Deprecated
  public static final String ADMIN_ACCUMULO_CLASSPATH_DEFAULT = "";

  // Worker properties
  private static final String WORKER_PREFIX = FLUO_PREFIX + ".worker";
  public static final String WORKER_NUM_THREADS_PROP = WORKER_PREFIX + ".num.threads";
  public static final int WORKER_NUM_THREADS_DEFAULT = 10;

  // Loader properties
  private static final String LOADER_PREFIX = FLUO_PREFIX + ".loader";
  public static final String LOADER_NUM_THREADS_PROP = LOADER_PREFIX + ".num.threads";
  public static final String LOADER_QUEUE_SIZE_PROP = LOADER_PREFIX + ".queue.size";
  public static final int LOADER_NUM_THREADS_DEFAULT = 10;
  public static final int LOADER_QUEUE_SIZE_DEFAULT = 10;

  // MiniFluo properties
  private static final String MINI_PREFIX = FLUO_PREFIX + ".mini";
  public static final String MINI_START_ACCUMULO_PROP = MINI_PREFIX + ".start.accumulo";
  public static final String MINI_DATA_DIR_PROP = MINI_PREFIX + ".data.dir";
  public static final boolean MINI_START_ACCUMULO_DEFAULT = true;
  public static final String MINI_DATA_DIR_DEFAULT = "${env:FLUO_HOME}/mini";

  /** The properties below get loaded into/from Zookeeper */
  // Observer properties
  public static final String OBSERVER_PREFIX = FLUO_PREFIX + ".observer.";
  /**
   * @since 1.1.0
   */
  public static final String OBSERVER_PROVIDER = FLUO_PREFIX + ".observer.provider";
  /**
   * @since 1.2.0
   */
  public static final String OBSERVER_INIT_DIR_PROP = FLUO_PREFIX + ".observer.init.dir";
  /**
   * @since 1.2.0
   */
  public static final String OBSERVER_JARS_URL_PROP = FLUO_PREFIX + ".observer.jars.url";
  // Observer defaults
  public static final String OBSERVER_PROVIDER_DEFAULT = "";
  public static final String OBSERVER_INIT_DIR_DEFAULT = "";
  public static final String OBSERVER_JARS_URL_DEFAULT = "";

  // Transaction
  public static final String TRANSACTION_PREFIX = FLUO_PREFIX + ".tx";
  public static final String TRANSACTION_ROLLBACK_TIME_PROP = TRANSACTION_PREFIX + ".rollback.time";
  public static final long TRANSACTION_ROLLBACK_TIME_DEFAULT = 300000;

  // Metrics
  public static final String REPORTER_PREFIX = FLUO_PREFIX + ".metrics.reporter";

  // Application config
  public static final String APP_PREFIX = FLUO_PREFIX + ".app";

  public FluoConfiguration() {
    super();
  }

  public FluoConfiguration(SimpleConfiguration other) {
    super(other);
  }

  public FluoConfiguration(InputStream in) {
    super(in);
  }

  public FluoConfiguration(File propertiesFile) {
    super(propertiesFile);
  }

  public FluoConfiguration(Map<String, String> map) {
    super(map);
  }

  public void validate() {
    // keep in alphabetical order
    getAccumuloInstance();
    getAccumuloPassword();
    getAccumuloTable();
    getAccumuloUser();
    getAccumuloZookeepers();
    getApplicationName();
    getAppZookeepers();
    getConnectionRetryTimeout();
    getLoaderQueueSize();
    getLoaderThreads();
    getObserverSpecifications();
    getTransactionRollbackTime();
    getWorkerThreads();
    getZookeeperTimeout();
  }

  public FluoConfiguration setApplicationName(String applicationName) {
    verifyApplicationName(applicationName);
    setProperty(CONNECTION_APPLICATION_NAME_PROP, applicationName);
    return this;
  }

  public String getApplicationName() {
    String applicationName;
    if (containsKey(CONNECTION_APPLICATION_NAME_PROP)) {
      applicationName = getString(CONNECTION_APPLICATION_NAME_PROP);
    } else if (containsKey(CLIENT_APPLICATION_NAME_PROP)) {
      applicationName = getString(CLIENT_APPLICATION_NAME_PROP);
    } else {
      throw new NoSuchElementException(CONNECTION_APPLICATION_NAME_PROP + " was not set");
    }
    verifyApplicationName(applicationName);
    return applicationName;
  }

  /**
   * Verifies application name. Avoids characters that Zookeeper does not like in nodes & Hadoop
   * does not like in HDFS paths.
   *
   * @param name Application name
   */
  private void verifyApplicationName(String name) {
    if (name == null) {
      throw new IllegalArgumentException("Application name cannot be null");
    }
    if (name.length() == 0) {
      throw new IllegalArgumentException("Application name length must be > 0");
    }
    String reason = null;
    char[] chars = name.toCharArray();
    char c;
    for (int i = 0; i < chars.length; i++) {
      c = chars[i];
      if (c == 0) {
        reason = "null character not allowed @" + i;
        break;
      } else if (c == '/' || c == '.' || c == ':') {
        reason = "invalid character '" + c + "'";
        break;
      } else if (c > '\u0000' && c <= '\u001f' || c >= '\u007f' && c <= '\u009F' || c >= '\ud800'
          && c <= '\uf8ff' || c >= '\ufff0' && c <= '\uffff') {
        reason = "invalid character @" + i;
        break;
      }
    }
    if (reason != null) {
      throw new IllegalArgumentException("Invalid application name \"" + name + "\" caused by "
          + reason);
    }
  }

  public FluoConfiguration setInstanceZookeepers(String zookeepers) {
    return setNonEmptyString(CONNECTION_ZOOKEEPERS_PROP, zookeepers);
  }

  public String getInstanceZookeepers() {
    return getDepNonEmptyString(CONNECTION_ZOOKEEPERS_PROP, CLIENT_ZOOKEEPER_CONNECT_PROP,
        CONNECTION_ZOOKEEPERS_DEFAULT);
  }

  public String getAppZookeepers() {
    return getInstanceZookeepers() + "/" + getApplicationName();
  }

  public FluoConfiguration setZookeeperTimeout(int timeout) {
    return setPositiveInt(CONNECTION_ZOOKEEPER_TIMEOUT_PROP, timeout);
  }

  public int getZookeeperTimeout() {
    return getDepPositiveInt(CONNECTION_ZOOKEEPER_TIMEOUT_PROP, CLIENT_ZOOKEEPER_TIMEOUT_PROP,
        CONNECTION_ZOOKEEPER_TIMEOUT_DEFAULT);
  }

  @Deprecated
  public FluoConfiguration setClientRetryTimeout(int timeoutMs) {
    return setConnectionRetryTimeout(timeoutMs);
  }

  @Deprecated
  public int getClientRetryTimeout() {
    return getConnectionRetryTimeout();
  }

  /**
   * @since 1.2.0
   */
  public FluoConfiguration setConnectionRetryTimeout(int timeoutMS) {
    Preconditions.checkArgument(timeoutMS >= -1, CONNECTION_RETRY_TIMEOUT_MS_PROP
        + " must be >= -1");
    setProperty(CONNECTION_RETRY_TIMEOUT_MS_PROP, timeoutMS);
    return this;
  }

  /**
   * @since 1.2.0
   */
  public int getConnectionRetryTimeout() {
    int retval;
    if (containsKey(CONNECTION_RETRY_TIMEOUT_MS_PROP)) {
      retval = getInt(CONNECTION_RETRY_TIMEOUT_MS_PROP, CONNECTION_RETRY_TIMEOUT_MS_DEFAULT);
    } else {
      retval = getInt(CLIENT_RETRY_TIMEOUT_MS_PROP, CONNECTION_RETRY_TIMEOUT_MS_DEFAULT);
    }
    Preconditions.checkArgument(retval >= -1, CONNECTION_RETRY_TIMEOUT_MS_PROP + " must be >= -1");
    return retval;
  }

  public FluoConfiguration setAccumuloInstance(String accumuloInstance) {
    return setNonEmptyString(ACCUMULO_INSTANCE_PROP, accumuloInstance);
  }

  public String getAccumuloInstance() {
    return getDepNonEmptyString(ACCUMULO_INSTANCE_PROP, CLIENT_ACCUMULO_INSTANCE_PROP);
  }

  public FluoConfiguration setAccumuloUser(String accumuloUser) {
    return setNonEmptyString(ACCUMULO_USER_PROP, accumuloUser);
  }

  public String getAccumuloUser() {
    return getDepNonEmptyString(ACCUMULO_USER_PROP, CLIENT_ACCUMULO_USER_PROP);
  }

  public FluoConfiguration setAccumuloPassword(String accumuloPassword) {
    setProperty(ACCUMULO_PASSWORD_PROP, verifyNotNull(ACCUMULO_PASSWORD_PROP, accumuloPassword));
    return this;
  }

  public String getAccumuloPassword() {
    if (containsKey(ACCUMULO_PASSWORD_PROP)) {
      return verifyNotNull(ACCUMULO_PASSWORD_PROP, getString(ACCUMULO_PASSWORD_PROP));
    } else if (containsKey(CLIENT_ACCUMULO_PASSWORD_PROP)) {
      return verifyNotNull(CLIENT_ACCUMULO_PASSWORD_PROP, getString(CLIENT_ACCUMULO_PASSWORD_PROP));
    }
    throw new NoSuchElementException(ACCUMULO_PASSWORD_PROP + " is not set!");
  }

  public FluoConfiguration setAccumuloZookeepers(String zookeepers) {
    return setNonEmptyString(ACCUMULO_ZOOKEEPERS_PROP, zookeepers);
  }

  public String getAccumuloZookeepers() {
    return getDepNonEmptyString(ACCUMULO_ZOOKEEPERS_PROP, CLIENT_ACCUMULO_ZOOKEEPERS_PROP,
        ACCUMULO_ZOOKEEPERS_DEFAULT);
  }

  /**
   * Sets Accumulo table. This property only needs to be set for FluoAdmin as it will be stored in
   * retrieved from Zookeeper for clients.
   */
  public FluoConfiguration setAccumuloTable(String table) {
    return setNonEmptyString(ACCUMULO_TABLE_PROP, table);
  }

  public String getAccumuloTable() {
    return getDepNonEmptyString(ACCUMULO_TABLE_PROP, ADMIN_ACCUMULO_TABLE_PROP);
  }

  @Deprecated
  public FluoConfiguration setAccumuloClasspath(String path) {
    setProperty(ADMIN_ACCUMULO_CLASSPATH_PROP, verifyNotNull(ADMIN_ACCUMULO_CLASSPATH_PROP, path));
    return this;
  }

  @Deprecated
  public String getAccumuloClasspath() {
    return getString(ADMIN_ACCUMULO_CLASSPATH_PROP, ADMIN_ACCUMULO_CLASSPATH_DEFAULT);
  }

  /**
   * Sets paths to jars to provide to Accumulo. If not set, Fluo will find jars on classpath
   *
   * @param path CSV list of paths
   * @since 1.2.0
   */
  public FluoConfiguration setAccumuloJars(String path) {
    setProperty(ACCUMULO_JARS_PROP, verifyNotNull(ACCUMULO_JARS_PROP, path));
    return this;
  }

  /**
   * Gets CSV list of jar paths to provide to Accumulo
   *
   * @since 1.2.0
   */
  public String getAccumuloJars() {
    return getString(ACCUMULO_JARS_PROP, ACCUMULO_JARS_DEFAULT);
  }

  /**
   * @since 1.2.0
   */
  public FluoConfiguration setDfsRoot(String dfsRoot) {
    return setNonEmptyString(DFS_ROOT_PROP, dfsRoot);
  }

  /**
   * @since 1.2.0
   */
  public String getDfsRoot() {
    return getNonEmptyString(DFS_ROOT_PROP, DFS_ROOT_DEFAULT);
  }

  public FluoConfiguration setWorkerThreads(int numThreads) {
    return setPositiveInt(WORKER_NUM_THREADS_PROP, numThreads);
  }

  public int getWorkerThreads() {
    return getPositiveInt(WORKER_NUM_THREADS_PROP, WORKER_NUM_THREADS_DEFAULT);
  }

  /**
   * @deprecated since 1.1.0. Replaced by {@link #setObserverProvider(String)} and
   *             {@link #getObserverProvider()}
   */
  @Deprecated
  public List<ObserverSpecification> getObserverSpecifications() {

    List<ObserverSpecification> configList = new ArrayList<>();
    Iterator<String> iter = getKeys();

    while (iter.hasNext()) {
      String key = iter.next();
      if (key.startsWith(FluoConfiguration.OBSERVER_PREFIX)
          && !key.equals(FluoConfiguration.OBSERVER_PROVIDER)
          && !key.equals(FluoConfiguration.OBSERVER_INIT_DIR_PROP)
          && !key.equals(FluoConfiguration.OBSERVER_JARS_URL_PROP)) {
        String value = getString(key).trim();

        if (value.isEmpty()) {
          throw new IllegalArgumentException(key + " is set to empty value");
        }

        String[] fields = value.split(",");
        if (fields.length == 0) {
          throw new IllegalArgumentException(key + " has bad value: " + value);
        }

        String className = fields[0];
        if (className.isEmpty()) {
          throw new IllegalArgumentException(key + " has empty class name: " + className);
        }

        Map<String, String> params = new HashMap<>();
        for (int i = 1; i < fields.length; i++) {
          String[] kv = fields[i].split("=");
          if (kv.length != 2) {
            throw new IllegalArgumentException(key
                + " has invalid param. Expected 'key=value' but encountered '" + fields[i] + "'");
          }
          if (kv[0].isEmpty() || kv[1].isEmpty()) {
            throw new IllegalArgumentException(key + " has empty key or value in param: "
                + fields[i]);
          }
          params.put(kv[0], kv[1]);
        }

        ObserverSpecification observerSpecification = new ObserverSpecification(className, params);
        configList.add(observerSpecification);
      }
    }
    return configList;
  }

  private int getNextObserverId() {
    Iterator<String> iter1 = getKeys(OBSERVER_PREFIX.substring(0, OBSERVER_PREFIX.length() - 1));
    int max = -1;
    while (iter1.hasNext()) {
      String key = iter1.next();
      String suffix = key.substring(OBSERVER_PREFIX.length());
      if (suffix.matches("\\d+")) {
        try {
          max = Math.max(max, Integer.parseInt(suffix));
        } catch (NumberFormatException e) {
          // not a number so ignore it... will not conflict with the number used later
        }
      }
    }

    return max + 1;
  }

  /**
   * Configure the observer provider that Fluo workers will use.
   *
   * @since 1.1.0
   *
   * @param className Name of a class that implements {@link ObserverProvider}. Must be non-null and
   *        non-empty.
   */
  public void setObserverProvider(String className) {
    setNonEmptyString(OBSERVER_PROVIDER, className);
  }

  /**
   * Calls {@link #setObserverProvider(String)} with the class name.
   *
   * @since 1.1.0
   */
  public void setObserverProvider(Class<? extends ObserverProvider> clazz) {
    setObserverProvider(clazz.getName());
  }

  /**
   * Sets directory where observers jars can found for initialization
   *
   * @param observerDir Path to directory
   * @since 1.2.0
   */
  public FluoConfiguration setObserverInitDir(String observerDir) {
    setProperty(OBSERVER_INIT_DIR_PROP, verifyNotNull(OBSERVER_INIT_DIR_PROP, observerDir));
    return this;
  }

  /**
   * Gets directory where observer jars can be found for initialization
   *
   * @return Path to directory
   * @since 1.2.0
   */
  public String getObserverInitDir() {
    return getString(OBSERVER_INIT_DIR_PROP, OBSERVER_INIT_DIR_DEFAULT);
  }

  /**
   * Sets URL to directory where observer jars can be found
   *
   * @param observerJarsUrl URL to observer jars directory
   * @since 1.2.0
   */
  public FluoConfiguration setObserverJarsUrl(String observerJarsUrl) {
    setProperty(OBSERVER_JARS_URL_PROP, verifyNotNull(OBSERVER_JARS_URL_PROP, observerJarsUrl));
    return this;
  }

  /**
   * @since 1.2.0
   */
  public String getObserverJarsUrl() {
    return getString(OBSERVER_JARS_URL_PROP, OBSERVER_JARS_URL_DEFAULT);
  }

  /**
   * @return The configured {@link ObserverProvider} class name. If one was not configured, returns
   *         {@value #OBSERVER_PROVIDER_DEFAULT}
   * @since 1.1.0
   */
  public String getObserverProvider() {
    return getString(OBSERVER_PROVIDER, OBSERVER_PROVIDER_DEFAULT);
  }

  @Deprecated
  private void addObserver(ObserverSpecification oconf, int next) {
    Map<String, String> params = oconf.getConfiguration().toMap();
    StringBuilder paramString = new StringBuilder();
    for (java.util.Map.Entry<String, String> pentry : params.entrySet()) {
      paramString.append(',');
      paramString.append(pentry.getKey());
      paramString.append('=');
      paramString.append(pentry.getValue());
    }
    setProperty(OBSERVER_PREFIX + "" + next, oconf.getClassName() + paramString);
  }

  /**
   * Adds an {@link ObserverSpecification} to the configuration using a unique integer prefix thats
   * not currently in use.
   *
   * @deprecated since 1.1.0. Replaced by {@link #setObserverProvider(String)} and
   *             {@link #getObserverProvider()}
   */
  @Deprecated
  public FluoConfiguration addObserver(ObserverSpecification oconf) {
    int next = getNextObserverId();
    addObserver(oconf, next);
    return this;
  }

  /**
   * Adds multiple observers using unique integer prefixes for each.
   *
   * @deprecated since 1.1.0. Replaced by {@link #setObserverProvider(String)} and
   *             {@link #getObserverProvider()}
   */
  @Deprecated
  public FluoConfiguration addObservers(Iterable<ObserverSpecification> observers) {
    int next = getNextObserverId();
    for (ObserverSpecification oconf : observers) {
      addObserver(oconf, next++);
    }
    return this;
  }

  /**
   * Removes any configured observers.
   *
   * @deprecated since 1.1.0. Replaced by {@link #setObserverProvider(String)} and
   *             {@link #getObserverProvider()}
   */
  @Deprecated
  public FluoConfiguration clearObservers() {
    Iterator<String> iter1 = getKeys(OBSERVER_PREFIX.substring(0, OBSERVER_PREFIX.length() - 1));
    while (iter1.hasNext()) {
      String key = iter1.next();
      clearProperty(key);
    }

    return this;
  }

  public FluoConfiguration setTransactionRollbackTime(long time, TimeUnit tu) {
    return setPositiveLong(TRANSACTION_ROLLBACK_TIME_PROP, tu.toMillis(time));
  }

  public long getTransactionRollbackTime() {
    return getPositiveLong(TRANSACTION_ROLLBACK_TIME_PROP, TRANSACTION_ROLLBACK_TIME_DEFAULT);
  }

  public FluoConfiguration setLoaderThreads(int numThreads) {
    return setNonNegativeInt(LOADER_NUM_THREADS_PROP, numThreads);
  }

  public int getLoaderThreads() {
    return getNonNegativeInt(LOADER_NUM_THREADS_PROP, LOADER_NUM_THREADS_DEFAULT);
  }

  public FluoConfiguration setLoaderQueueSize(int queueSize) {
    return setNonNegativeInt(LOADER_QUEUE_SIZE_PROP, queueSize);
  }

  public int getLoaderQueueSize() {
    return getNonNegativeInt(LOADER_QUEUE_SIZE_PROP, LOADER_QUEUE_SIZE_DEFAULT);
  }

  /**
   * @param reporter The name of the reporter to get configuration for, i.e. console, jmx, graphite.
   * @return A subset of this configuration using the prefix {@value #REPORTER_PREFIX} with the
   *         reporter parameter appended. Any change made to subset will be reflected in this
   *         configuration, but with the prefix added.
   */
  public SimpleConfiguration getReporterConfiguration(String reporter) {
    return subset(REPORTER_PREFIX + "." + reporter);
  }

  /**
   * @return A subset of this configuration using the prefix {@value #APP_PREFIX}. Any change made
   *         to subset will be reflected in this configuration, but with the prefix added. This
   *         method is useful for setting application configuration before initialization. For
   *         reading application configuration after initialization, see
   *         {@link FluoClient#getAppConfiguration()} and {@link Context#getAppConfiguration()}
   */
  public SimpleConfiguration getAppConfiguration() {
    return subset(APP_PREFIX);
  }

  public FluoConfiguration setMiniStartAccumulo(boolean startAccumulo) {
    setProperty(MINI_START_ACCUMULO_PROP, startAccumulo);
    return this;
  }

  public boolean getMiniStartAccumulo() {
    return getBoolean(MINI_START_ACCUMULO_PROP, MINI_START_ACCUMULO_DEFAULT);
  }

  public FluoConfiguration setMiniDataDir(String dataDir) {
    return setNonEmptyString(MINI_DATA_DIR_PROP, dataDir);
  }

  public String getMiniDataDir() {
    return getNonEmptyString(MINI_DATA_DIR_PROP, MINI_DATA_DIR_DEFAULT);
  }

  /**
   * Logs all properties
   */
  public void print() {
    Iterator<String> iter = getKeys();
    while (iter.hasNext()) {
      String key = iter.next();
      log.info(key + " = " + getRawString(key));
    }
  }

  private boolean verifyStringPropSet(String... keys) {
    for (String key : keys) {
      if (containsKey(key) && !getString(key).isEmpty()) {
        return true;
      }
    }
    log.info(keys[0] + " is not set");
    return false;
  }

  private boolean verifyStringPropNotSet(String... keys) {
    for (String key : keys) {
      if (containsKey(key) && !getString(key).isEmpty()) {
        log.info(key + " should not be set");
        return false;
      }
    }
    return true;
  }

  public boolean hasRequiredConnectionProps() {
    boolean valid = true;
    valid &= verifyStringPropSet(CONNECTION_APPLICATION_NAME_PROP, CLIENT_APPLICATION_NAME_PROP);
    return valid;
  }

  /**
   * Returns true if required properties for FluoClient are set
   */
  public boolean hasRequiredClientProps() {
    boolean valid = true;
    valid &= verifyStringPropSet(CONNECTION_APPLICATION_NAME_PROP, CLIENT_APPLICATION_NAME_PROP);
    valid &= verifyStringPropSet(ACCUMULO_USER_PROP, CLIENT_ACCUMULO_USER_PROP);
    valid &= verifyStringPropSet(ACCUMULO_PASSWORD_PROP, CLIENT_ACCUMULO_PASSWORD_PROP);
    valid &= verifyStringPropSet(ACCUMULO_INSTANCE_PROP, CLIENT_ACCUMULO_INSTANCE_PROP);
    return valid;
  }

  /**
   * Returns true if required properties for FluoAdmin are set
   */
  public boolean hasRequiredAdminProps() {
    boolean valid = true;
    valid &= hasRequiredClientProps();
    valid &= verifyStringPropSet(ACCUMULO_TABLE_PROP, ADMIN_ACCUMULO_TABLE_PROP);
    return valid;
  }

  /**
   * Returns true if required properties for Oracle are set
   */
  public boolean hasRequiredOracleProps() {
    boolean valid = true;
    valid &= hasRequiredClientProps();
    return valid;
  }

  /**
   * Returns true if required properties for Worker are set
   */
  public boolean hasRequiredWorkerProps() {
    boolean valid = true;
    valid &= hasRequiredClientProps();
    return valid;
  }

  /**
   * Returns true if required properties for MiniFluo are set
   */
  public boolean hasRequiredMiniFluoProps() {
    boolean valid = true;
    if (getMiniStartAccumulo()) {
      // ensure that client properties are not set since we are using MiniAccumulo
      valid &= verifyStringPropNotSet(ACCUMULO_USER_PROP, CLIENT_ACCUMULO_USER_PROP);
      valid &= verifyStringPropNotSet(ACCUMULO_PASSWORD_PROP, CLIENT_ACCUMULO_PASSWORD_PROP);
      valid &= verifyStringPropNotSet(ACCUMULO_INSTANCE_PROP, CLIENT_ACCUMULO_INSTANCE_PROP);
      valid &= verifyStringPropNotSet(ACCUMULO_ZOOKEEPERS_PROP, CLIENT_ACCUMULO_ZOOKEEPERS_PROP);
      valid &= verifyStringPropNotSet(CONNECTION_ZOOKEEPERS_PROP, CLIENT_ZOOKEEPER_CONNECT_PROP);
      if (valid == false) {
        log.error("Client properties should not be set in your configuration if MiniFluo is "
            + "configured to start its own accumulo (indicated by fluo.mini.start.accumulo being "
            + "set to true)");
      }
    } else {
      valid &= hasRequiredClientProps();
      valid &= hasRequiredAdminProps();
      valid &= hasRequiredOracleProps();
      valid &= hasRequiredWorkerProps();
    }
    return valid;
  }

  public SimpleConfiguration getClientConfiguration() {
    SimpleConfiguration clientConfig = new SimpleConfiguration();
    Iterator<String> iter = getKeys();
    while (iter.hasNext()) {
      String key = iter.next();
      if (key.startsWith(CONNECTION_PREFIX) || key.startsWith(ACCUMULO_PREFIX)
          || key.startsWith(CLIENT_PREFIX)) {
        clientConfig.setProperty(key, getRawString(key));
      }
    }
    return clientConfig;
  }

  /**
   * Returns configuration with all Fluo properties set to their default. NOTE - some properties do
   * not have defaults and will not be set.
   */
  public static SimpleConfiguration getDefaultConfiguration() {
    SimpleConfiguration config = new SimpleConfiguration();
    setDefaultConfiguration(config);
    return config;
  }

  /**
   * Sets all Fluo properties to their default in the given configuration. NOTE - some properties do
   * not have defaults and will not be set.
   */
  public static void setDefaultConfiguration(SimpleConfiguration config) {
    config.setProperty(CONNECTION_ZOOKEEPERS_PROP, CONNECTION_ZOOKEEPERS_DEFAULT);
    config.setProperty(CONNECTION_ZOOKEEPER_TIMEOUT_PROP, CONNECTION_ZOOKEEPER_TIMEOUT_DEFAULT);
    config.setProperty(DFS_ROOT_PROP, DFS_ROOT_DEFAULT);
    config.setProperty(ACCUMULO_ZOOKEEPERS_PROP, ACCUMULO_ZOOKEEPERS_DEFAULT);
    config.setProperty(WORKER_NUM_THREADS_PROP, WORKER_NUM_THREADS_DEFAULT);
    config.setProperty(TRANSACTION_ROLLBACK_TIME_PROP, TRANSACTION_ROLLBACK_TIME_DEFAULT);
    config.setProperty(LOADER_NUM_THREADS_PROP, LOADER_NUM_THREADS_DEFAULT);
    config.setProperty(LOADER_QUEUE_SIZE_PROP, LOADER_QUEUE_SIZE_DEFAULT);
    config.setProperty(MINI_START_ACCUMULO_PROP, MINI_START_ACCUMULO_DEFAULT);
    config.setProperty(MINI_DATA_DIR_PROP, MINI_DATA_DIR_DEFAULT);
  }

  private FluoConfiguration setNonNegativeInt(String property, int value) {
    Preconditions.checkArgument(value >= 0, property + " must be non-negative");
    setProperty(property, value);
    return this;
  }

  private int getNonNegativeInt(String property, int defaultValue) {
    int value = getInt(property, defaultValue);
    Preconditions.checkArgument(value >= 0, property + " must be non-negative");
    return value;
  }

  private FluoConfiguration setPositiveInt(String property, int value) {
    Preconditions.checkArgument(value > 0, property + " must be positive");
    setProperty(property, value);
    return this;
  }

  private int getPositiveInt(String property, int defaultValue) {
    int value = getInt(property, defaultValue);
    Preconditions.checkArgument(value > 0, property + " must be positive");
    return value;
  }

  private int getDepPositiveInt(String property, String depProperty, int defaultValue) {
    if (containsKey(property)) {
      return getInt(property, defaultValue);
    } else {
      return getInt(depProperty, defaultValue);
    }
  }

  private FluoConfiguration setPositiveLong(String property, long value) {
    Preconditions.checkArgument(value > 0, property + " must be positive");
    setProperty(property, value);
    return this;
  }

  private long getPositiveLong(String property, long defaultValue) {
    long value = getLong(property, defaultValue);
    Preconditions.checkArgument(value > 0, property + " must be positive");
    return value;
  }

  private FluoConfiguration setNonEmptyString(String property, String value) {
    Objects.requireNonNull(value, property + " cannot be null");
    Preconditions.checkArgument(!value.isEmpty(), property + " cannot be empty");
    setProperty(property, value);
    return this;
  }

  private String getNonEmptyString(String property, String defaultValue) {
    String value = getString(property, defaultValue);
    Objects.requireNonNull(value, property + " cannot be null");
    Preconditions.checkArgument(!value.isEmpty(), property + " cannot be empty");
    return value;
  }

  private String getNonEmptyString(String property) {
    String value = getString(property);
    Objects.requireNonNull(value, property + " cannot be null");
    Preconditions.checkArgument(!value.isEmpty(), property + " cannot be empty");
    return value;
  }

  private String getDepNonEmptyString(String property, String depProperty, String defaultValue) {
    return containsKey(property) ? getNonEmptyString(property, defaultValue) : getNonEmptyString(
        depProperty, defaultValue);
  }

  private String getDepNonEmptyString(String property, String depProperty) {
    if (containsKey(property)) {
      return getNonEmptyString(property);
    } else if (containsKey(depProperty)) {
      return getNonEmptyString(depProperty);
    } else {
      throw new NoSuchElementException(property + " is not set!");
    }
  }

  private static String verifyNotNull(String property, String value) {
    Objects.requireNonNull(value, property + " cannot be null");
    return value;
  }
}
