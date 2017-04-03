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
  public static final String CLIENT_APPLICATION_NAME_PROP = CLIENT_PREFIX + ".application.name";
  public static final String CLIENT_ACCUMULO_PASSWORD_PROP = CLIENT_PREFIX + ".accumulo.password";
  public static final String CLIENT_ACCUMULO_USER_PROP = CLIENT_PREFIX + ".accumulo.user";
  public static final String CLIENT_ACCUMULO_INSTANCE_PROP = CLIENT_PREFIX + ".accumulo.instance";
  public static final String CLIENT_ACCUMULO_ZOOKEEPERS_PROP = CLIENT_PREFIX
      + ".accumulo.zookeepers";
  public static final String CLIENT_ZOOKEEPER_TIMEOUT_PROP = CLIENT_PREFIX + ".zookeeper.timeout";
  public static final String CLIENT_ZOOKEEPER_CONNECT_PROP = CLIENT_PREFIX + ".zookeeper.connect";
  public static final String CLIENT_RETRY_TIMEOUT_MS_PROP = CLIENT_PREFIX + ".retry.timeout.ms";
  public static final int CLIENT_ZOOKEEPER_TIMEOUT_DEFAULT = 30000;
  public static final String CLIENT_ACCUMULO_ZOOKEEPERS_DEFAULT = "localhost";
  public static final String CLIENT_ZOOKEEPER_CONNECT_DEFAULT = "localhost/fluo";
  public static final int CLIENT_RETRY_TIMEOUT_MS_DEFAULT = -1;

  // Administration
  private static final String ADMIN_PREFIX = FLUO_PREFIX + ".admin";
  public static final String ADMIN_ACCUMULO_TABLE_PROP = ADMIN_PREFIX + ".accumulo.table";
  public static final String ADMIN_ACCUMULO_CLASSPATH_PROP = ADMIN_PREFIX + ".accumulo.classpath";
  public static final String ADMIN_ACCUMULO_CLASSPATH_DEFAULT = "";

  // Worker
  private static final String WORKER_PREFIX = FLUO_PREFIX + ".worker";
  public static final String WORKER_NUM_THREADS_PROP = WORKER_PREFIX + ".num.threads";
  public static final int WORKER_NUM_THREADS_DEFAULT = 10;

  // Loader
  private static final String LOADER_PREFIX = FLUO_PREFIX + ".loader";
  public static final String LOADER_NUM_THREADS_PROP = LOADER_PREFIX + ".num.threads";
  public static final String LOADER_QUEUE_SIZE_PROP = LOADER_PREFIX + ".queue.size";
  public static final int LOADER_NUM_THREADS_DEFAULT = 10;
  public static final int LOADER_QUEUE_SIZE_DEFAULT = 10;

  // MiniFluo
  private static final String MINI_PREFIX = FLUO_PREFIX + ".mini";
  public static final String MINI_START_ACCUMULO_PROP = MINI_PREFIX + ".start.accumulo";
  public static final String MINI_DATA_DIR_PROP = MINI_PREFIX + ".data.dir";
  public static final boolean MINI_START_ACCUMULO_DEFAULT = true;
  public static final String MINI_DATA_DIR_DEFAULT = "${env:FLUO_HOME}/mini";

  /** The properties below get loaded into/from Zookeeper */
  // Observer
  @Deprecated
  public static final String OBSERVER_PREFIX = FLUO_PREFIX + ".observer.";

  /**
   * @since 1.1.0
   */
  public static final String OBSERVER_PROVIDER = FLUO_PREFIX + ".observer.provider";

  /**
   * @since 1.1.0
   */
  public static final String OBSERVER_PROVIDER_DEFAULT = "";

  // Transaction
  public static final String TRANSACTION_PREFIX = FLUO_PREFIX + ".tx";
  public static final String TRANSACTION_ROLLBACK_TIME_PROP = TRANSACTION_PREFIX + ".rollback.time";
  public static final long TRANSACTION_ROLLBACK_TIME_DEFAULT = 300000;

  // Metrics
  public static final String REPORTER_PREFIX = FLUO_PREFIX + ".metrics.reporter";

  // application config
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
    getAccumuloClasspath();
    getAccumuloInstance();
    getAccumuloPassword();
    getAccumuloTable();
    getAccumuloUser();
    getAccumuloZookeepers();
    getApplicationName();
    getAppZookeepers();
    getClientRetryTimeout();
    getLoaderQueueSize();
    getLoaderThreads();
    getObserverSpecifications();
    getTransactionRollbackTime();
    getWorkerThreads();
    getZookeeperTimeout();
  }

  public FluoConfiguration setApplicationName(String applicationName) {
    verifyApplicationName(applicationName);
    setProperty(CLIENT_APPLICATION_NAME_PROP, applicationName);
    return this;
  }

  public String getApplicationName() {
    String applicationName = getString(CLIENT_APPLICATION_NAME_PROP);
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
    return setNonEmptyString(CLIENT_ZOOKEEPER_CONNECT_PROP, zookeepers);
  }

  public String getInstanceZookeepers() {
    return getNonEmptyString(CLIENT_ZOOKEEPER_CONNECT_PROP, CLIENT_ZOOKEEPER_CONNECT_DEFAULT);
  }

  public String getAppZookeepers() {
    return getInstanceZookeepers() + "/" + getApplicationName();
  }

  public FluoConfiguration setZookeeperTimeout(int timeout) {
    return setPositiveInt(CLIENT_ZOOKEEPER_TIMEOUT_PROP, timeout);
  }

  public int getZookeeperTimeout() {
    return getPositiveInt(CLIENT_ZOOKEEPER_TIMEOUT_PROP, CLIENT_ZOOKEEPER_TIMEOUT_DEFAULT);
  }

  public FluoConfiguration setClientRetryTimeout(int timeoutMS) {
    Preconditions.checkArgument(timeoutMS >= -1, CLIENT_RETRY_TIMEOUT_MS_PROP + " must be >= -1");
    setProperty(CLIENT_RETRY_TIMEOUT_MS_PROP, timeoutMS);
    return this;
  }

  public int getClientRetryTimeout() {
    int retval = getInt(CLIENT_RETRY_TIMEOUT_MS_PROP, CLIENT_RETRY_TIMEOUT_MS_DEFAULT);
    Preconditions.checkArgument(retval >= -1, CLIENT_RETRY_TIMEOUT_MS_PROP + " must be >= -1");
    return retval;
  }

  public FluoConfiguration setAccumuloInstance(String accumuloInstance) {
    return setNonEmptyString(CLIENT_ACCUMULO_INSTANCE_PROP, accumuloInstance);
  }

  public String getAccumuloInstance() {
    return getNonEmptyString(CLIENT_ACCUMULO_INSTANCE_PROP);
  }

  public FluoConfiguration setAccumuloUser(String accumuloUser) {
    return setNonEmptyString(CLIENT_ACCUMULO_USER_PROP, accumuloUser);
  }

  public String getAccumuloUser() {
    return getNonEmptyString(CLIENT_ACCUMULO_USER_PROP);
  }

  public FluoConfiguration setAccumuloPassword(String accumuloPassword) {
    setProperty(CLIENT_ACCUMULO_PASSWORD_PROP,
        verifyNotNull(CLIENT_ACCUMULO_PASSWORD_PROP, accumuloPassword));
    return this;
  }

  public String getAccumuloPassword() {
    return verifyNotNull(CLIENT_ACCUMULO_PASSWORD_PROP, getString(CLIENT_ACCUMULO_PASSWORD_PROP));
  }

  public FluoConfiguration setAccumuloZookeepers(String zookeepers) {
    return setNonEmptyString(CLIENT_ACCUMULO_ZOOKEEPERS_PROP, zookeepers);
  }

  public String getAccumuloZookeepers() {
    return getNonEmptyString(CLIENT_ACCUMULO_ZOOKEEPERS_PROP, CLIENT_ACCUMULO_ZOOKEEPERS_DEFAULT);
  }

  /**
   * Sets Accumulo table. This property only needs to be set for FluoAdmin as it will be stored in
   * retrieved from Zookeeper for clients.
   */
  public FluoConfiguration setAccumuloTable(String table) {
    return setNonEmptyString(ADMIN_ACCUMULO_TABLE_PROP, table);
  }

  public String getAccumuloTable() {
    return getNonEmptyString(ADMIN_ACCUMULO_TABLE_PROP);
  }

  public FluoConfiguration setAccumuloClasspath(String path) {
    setProperty(ADMIN_ACCUMULO_CLASSPATH_PROP, verifyNotNull(ADMIN_ACCUMULO_CLASSPATH_PROP, path));
    return this;
  }

  public String getAccumuloClasspath() {
    return getString(ADMIN_ACCUMULO_CLASSPATH_PROP, ADMIN_ACCUMULO_CLASSPATH_DEFAULT);
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
          && !key.equals(FluoConfiguration.OBSERVER_PROVIDER)) {
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

  private boolean verifyStringPropSet(String key) {
    if (containsKey(key) && !getString(key).isEmpty()) {
      return true;
    }
    log.info(key + " is not set");
    return false;
  }

  private boolean verifyStringPropNotSet(String key) {
    if (containsKey(key) && !getString(key).isEmpty()) {
      log.info(key + " should not be set");
      return false;
    }
    return true;
  }

  /**
   * Returns true if required properties for FluoClient are set
   */
  public boolean hasRequiredClientProps() {
    boolean valid = true;
    valid &= verifyStringPropSet(CLIENT_APPLICATION_NAME_PROP);
    valid &= verifyStringPropSet(CLIENT_ACCUMULO_USER_PROP);
    valid &= verifyStringPropSet(CLIENT_ACCUMULO_PASSWORD_PROP);
    valid &= verifyStringPropSet(CLIENT_ACCUMULO_INSTANCE_PROP);
    return valid;
  }

  /**
   * Returns true if required properties for FluoAdmin are set
   */
  public boolean hasRequiredAdminProps() {
    boolean valid = true;
    valid &= hasRequiredClientProps();
    valid &= verifyStringPropSet(ADMIN_ACCUMULO_TABLE_PROP);
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
      valid &= verifyStringPropNotSet(CLIENT_ACCUMULO_USER_PROP);
      valid &= verifyStringPropNotSet(CLIENT_ACCUMULO_PASSWORD_PROP);
      valid &= verifyStringPropNotSet(CLIENT_ACCUMULO_INSTANCE_PROP);
      valid &= verifyStringPropNotSet(CLIENT_ACCUMULO_ZOOKEEPERS_PROP);
      valid &= verifyStringPropNotSet(CLIENT_ZOOKEEPER_CONNECT_PROP);
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
      if (key.startsWith(CLIENT_PREFIX)) {
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
    config.setProperty(CLIENT_ZOOKEEPER_CONNECT_PROP, CLIENT_ZOOKEEPER_CONNECT_DEFAULT);
    config.setProperty(CLIENT_ZOOKEEPER_TIMEOUT_PROP, CLIENT_ZOOKEEPER_TIMEOUT_DEFAULT);
    config.setProperty(CLIENT_ACCUMULO_ZOOKEEPERS_PROP, CLIENT_ACCUMULO_ZOOKEEPERS_DEFAULT);
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

  private static String verifyNotNull(String property, String value) {
    Objects.requireNonNull(value, property + " cannot be null");
    return value;
  }
}
