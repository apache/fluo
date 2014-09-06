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
package io.fluo.api.config;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Fluo configuration helper class
 */
public class FluoConfiguration extends CompositeConfiguration {
  
  private static Logger log = LoggerFactory.getLogger(FluoConfiguration.class);
  
  public static final String FLUO_PREFIX = "io.fluo";

  // Client properties
  public static final String CLIENT_PREFIX = FLUO_PREFIX + ".client";
  public static final String CLIENT_ACCUMULO_PASSWORD_PROP = CLIENT_PREFIX + ".accumulo.password";
  public static final String CLIENT_ACCUMULO_USER_PROP = CLIENT_PREFIX + ".accumulo.user";
  public static final String CLIENT_ACCUMULO_INSTANCE_PROP = CLIENT_PREFIX + ".accumulo.instance";
  public static final String CLIENT_ZOOKEEPER_ROOT_PROP = CLIENT_PREFIX + ".zookeeper.root";
  public static final String CLIENT_ZOOKEEPER_TIMEOUT_PROP = CLIENT_PREFIX + ".zookeeper.timeout";
  public static final String CLIENT_ZOOKEEPER_CONNECT_PROP = CLIENT_PREFIX + ".zookeeper.connect";
  public static final String CLIENT_CLASS_PROP = CLIENT_PREFIX + ".class";
  public static final String CLIENT_ZOOKEEPER_ROOT_DEFAULT = "/fluo";
  public static final int CLIENT_ZOOKEEPER_TIMEOUT_DEFAULT = 30000;
  public static final String CLIENT_ZOOKEEPER_CONNECT_DEFAULT = "localhost";
  public static final String CLIENT_CLASS_DEFAULT = FLUO_PREFIX + ".core.client.FluoClientImpl";
  
  // Administration
  public static final String ADMIN_PREFIX = FLUO_PREFIX + ".admin";
  public static final String ADMIN_ZOOKEEPER_CLEAR_PROP = ADMIN_PREFIX + ".zookeeper.clear";
  public static final String ADMIN_ACCUMULO_TABLE_PROP = ADMIN_PREFIX + ".accumulo.table";
  public static final String ADMIN_ACCUMULO_CLASSPATH_PROP = ADMIN_PREFIX + ".accumulo.classpath";
  public static final String ADMIN_ACCUMULO_CLASSPATH_DEFAULT = "";
  public static final String ADMIN_CLASS_PROP = ADMIN_PREFIX + ".class";
  public static final boolean ADMIN_ZOOKEEPER_CLEAR_DEFAULT = false;
  public static final String ADMIN_CLASS_DEFAULT = FLUO_PREFIX + ".core.client.FluoAdminImpl";
  
  // Worker 
  public static final String WORKER_PREFIX = FLUO_PREFIX + ".worker";
  public static final String WORKER_NUM_THREADS_PROP = WORKER_PREFIX + ".num.threads";
  public static final String WORKER_INSTANCES_PROP = WORKER_PREFIX + ".instances";
  public static final String WORKER_MAX_MEMORY_MB_PROP = WORKER_PREFIX + ".max.memory.mb";
  public static final int WORKER_NUM_THREADS_DEFAULT = 10;
  public static final int WORKER_INSTANCES_DEFAULT = 1;
  public static final int WORKER_MAX_MEMORY_MB_DEFAULT = 256;
  
  // Loader 
  public static final String LOADER_PREFIX = FLUO_PREFIX + ".loader";
  public static final String LOADER_NUM_THREADS_PROP = LOADER_PREFIX + ".num.threads";
  public static final String LOADER_QUEUE_SIZE_PROP = LOADER_PREFIX + ".queue.size";
  public static final int LOADER_NUM_THREADS_DEFAULT = 10;
  public static final int LOADER_QUEUE_SIZE_DEFAULT = 10;
  
  // Oracle
  public static final String ORACLE_PREFIX = FLUO_PREFIX + ".oracle";
  public static final String ORACLE_PORT_PROP = ORACLE_PREFIX + ".port";
  public static final String ORACLE_MAX_MEMORY_MB_PROP = ORACLE_PREFIX + ".max.memory.mb";
  public static final int ORACLE_PORT_DEFAULT = 9913;
  public static final int ORACLE_MAX_MEMORY_MB_DEFAULT = 256;
  
  // MiniFluo
  public static final String MINI_PREFIX = FLUO_PREFIX + ".mini";
  public static final String MINI_CLASS_PROP = MINI_PREFIX + ".class";
  public static final String MINI_CLASS_DEFAULT = FLUO_PREFIX + ".core.client.MiniFluoImpl";
  
  /** The properties below get loaded into/from Zookeeper */
  // Observer
  public static final String OBSERVER_PREFIX = FLUO_PREFIX + ".observer.";
  
  // Transaction 
  public static final String TRANSACTION_PREFIX = FLUO_PREFIX + ".tx";
  public static final String TRANSACTION_ROLLBACK_TIME_PROP = TRANSACTION_PREFIX + ".rollback.time";
  public static final long TRANSACTION_ROLLBACK_TIME_DEFAULT = 300000;
  
  public FluoConfiguration() {
    super();
    setThrowExceptionOnMissing(true);
    setDelimiterParsingDisabled(true);
  }
  
  public FluoConfiguration(Configuration configuration) {
    this();
    addConfiguration(configuration);
  }
  
  public FluoConfiguration(File propertiesFile) {
    this();
    try {
      addConfiguration(new PropertiesConfiguration(propertiesFile));
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public FluoConfiguration setZookeepers(String zookeepers) {
    setProperty(CLIENT_ZOOKEEPER_CONNECT_PROP, zookeepers);
    return this;
  }
  
  public String getZookeepers() {
    return getString(CLIENT_ZOOKEEPER_CONNECT_PROP, CLIENT_ZOOKEEPER_CONNECT_DEFAULT);
  }
 
  public FluoConfiguration setZookeeperTimeout(int timeout) {
    setProperty(CLIENT_ZOOKEEPER_TIMEOUT_PROP, timeout);
    return this;
  }
    
  public int getZookeeperTimeout() {
    return getInt(CLIENT_ZOOKEEPER_TIMEOUT_PROP, CLIENT_ZOOKEEPER_TIMEOUT_DEFAULT);
  }
  
  public FluoConfiguration setZookeeperRoot(String zookeeperRoot) {
    setProperty(CLIENT_ZOOKEEPER_ROOT_PROP, zookeeperRoot);
    return this;
  }
  
  public String getZookeeperRoot() {
    return getString(CLIENT_ZOOKEEPER_ROOT_PROP, CLIENT_ZOOKEEPER_ROOT_DEFAULT);
  }
  
  public FluoConfiguration setAccumuloInstance(String accumuloInstance) {
    setProperty(CLIENT_ACCUMULO_INSTANCE_PROP, accumuloInstance);
    return this;
  }
    
  public String getAccumuloInstance() {
    return getString(CLIENT_ACCUMULO_INSTANCE_PROP);
  }
  
  public FluoConfiguration setAccumuloUser(String accumuloUser) {
    setProperty(CLIENT_ACCUMULO_USER_PROP, accumuloUser);
    return this;
  }
  
  public String getAccumuloUser() {
    return getString(CLIENT_ACCUMULO_USER_PROP);
  }
    
  public FluoConfiguration setAccumuloPassword(String accumuloPassword) {
    setProperty(CLIENT_ACCUMULO_PASSWORD_PROP, accumuloPassword);
    return this;
  }
  
  public String getAccumuloPassword() {
    return getString(CLIENT_ACCUMULO_PASSWORD_PROP);
  }
  
  public FluoConfiguration setClientClass(String clientClass) {
    setProperty(CLIENT_CLASS_PROP, clientClass);
    return this;
  }
  
  public String getClientClass() {
    return getString(CLIENT_CLASS_PROP, CLIENT_CLASS_DEFAULT);
  }
  
  /**
   * Sets Accumulo table.  This property only needs to 
   * be set for FluoAdmin as it will be stored in retrieved
   * from Zookeeper for clients.
   */
  public FluoConfiguration setAccumuloTable(String table) {
    setProperty(ADMIN_ACCUMULO_TABLE_PROP, table);
    return this;
  }
  
  public String getAccumuloTable() {
    return getString(ADMIN_ACCUMULO_TABLE_PROP);
  }
  
  public FluoConfiguration setAccumuloClasspath(String path) {
    setProperty(ADMIN_ACCUMULO_CLASSPATH_PROP, path);
    return this;
  }
  
  public String getAccumuloClasspath() {
    return getString(ADMIN_ACCUMULO_CLASSPATH_PROP, ADMIN_ACCUMULO_CLASSPATH_DEFAULT);
  }
 
  public FluoConfiguration setClearZookeeper(boolean clear) {
    setProperty(ADMIN_ZOOKEEPER_CLEAR_PROP, clear);
    return this;
  }
  
  public boolean getClearZookeeper() {
    return getBoolean(ADMIN_ZOOKEEPER_CLEAR_PROP, ADMIN_ZOOKEEPER_CLEAR_DEFAULT);
  }
  
  public FluoConfiguration setAdminClass(String adminClass) {
    setProperty(ADMIN_CLASS_PROP, adminClass);
    return this;
  }
  
  public String getAdminClass() {
    return getString(ADMIN_CLASS_PROP, ADMIN_CLASS_DEFAULT);
  }
  
  public FluoConfiguration setWorkerThreads(int numThreads) {
    if (numThreads <= 0)
      throw new IllegalArgumentException("Must be positive " + numThreads);
    setProperty(WORKER_NUM_THREADS_PROP, numThreads);
    return this;
  }
  
  public int getWorkerThreads() {
    return getInt(WORKER_NUM_THREADS_PROP, WORKER_NUM_THREADS_DEFAULT);
  }

  public FluoConfiguration setObservers(List<ObserverConfiguration> observers) {
    
    Iterator<String> iter1 = getKeys(OBSERVER_PREFIX);
    while (iter1.hasNext()) {
      String key = iter1.next();
      if (key.substring(OBSERVER_PREFIX.length()).matches("\\d+")) {
        clearProperty(key);
      }
    }

    int count = 0;
    for (ObserverConfiguration oconf : observers) {
      Map<String,String> params = oconf.getParameters();
      StringBuilder paramString = new StringBuilder();
      for (java.util.Map.Entry<String,String> pentry : params.entrySet()) {
        paramString.append(',');
        paramString.append(pentry.getKey());
        paramString.append('=');
        paramString.append(pentry.getValue());
      }
      setProperty(OBSERVER_PREFIX + "" + count, oconf.getClassName() + paramString);
      count++;
    }
    return this;
  }

  public void setTransactionRollbackTime(long time, TimeUnit tu) {
    setProperty(TRANSACTION_ROLLBACK_TIME_PROP, tu.toMillis(time));
  }
  
  public long getTransactionRollbackTime() {
    return getLong(TRANSACTION_ROLLBACK_TIME_PROP, TRANSACTION_ROLLBACK_TIME_DEFAULT);
  }

  public FluoConfiguration setWorkerInstances(int workerInstances) {
    setProperty(WORKER_INSTANCES_PROP, workerInstances);
    return this;
  }

  public int getWorkerInstances() {
    return getInt(WORKER_INSTANCES_PROP, WORKER_INSTANCES_DEFAULT);
  }
  
  public FluoConfiguration setWorkerMaxMemory(int maxMemoryMB) {
    setProperty(WORKER_MAX_MEMORY_MB_PROP, maxMemoryMB);
    return this;
  }
  
  public int getWorkerMaxMemory() {
    return getInt(WORKER_MAX_MEMORY_MB_PROP, WORKER_MAX_MEMORY_MB_DEFAULT);
  }
  
  public FluoConfiguration setLoaderThreads(int numThreads) {
    setProperty(LOADER_NUM_THREADS_PROP, numThreads);
    return this;
  }
  
  public int getLoaderThreads() {
    return getInt(LOADER_NUM_THREADS_PROP, LOADER_NUM_THREADS_DEFAULT);
  }
  
  public FluoConfiguration setLoaderQueueSize(int queueSize) {
    setProperty(LOADER_QUEUE_SIZE_PROP, queueSize);
    return this;
  }
  
  public int getLoaderQueueSize() {
    return getInt(LOADER_QUEUE_SIZE_PROP, LOADER_QUEUE_SIZE_DEFAULT);
  }
  
  public FluoConfiguration setOracleMaxMemory(int oracleMaxMemory) {
    setProperty(ORACLE_MAX_MEMORY_MB_PROP, oracleMaxMemory);
    return this;
  }
  
  public int getOracleMaxMemory() {
    return getInt(ORACLE_MAX_MEMORY_MB_PROP, ORACLE_MAX_MEMORY_MB_DEFAULT);
  }

  public FluoConfiguration setOraclePort(int oraclePort) {
    setProperty(ORACLE_PORT_PROP, oraclePort);
    return this;
  }
  
  public int getOraclePort() {
    return getInt(ORACLE_PORT_PROP, ORACLE_PORT_DEFAULT);
  }
  
  public FluoConfiguration setMiniClass(String miniClass) {
    setProperty(MINI_CLASS_PROP, miniClass);
    return this;
  }
  
  public String getMiniClass() {
    return getString(MINI_CLASS_PROP, MINI_CLASS_DEFAULT);
  }
      
  protected void setDefault(String key, String val) {
    if (getProperty(key) == null)
      setProperty(key, val);
  }

  /**
   * Logs all properties
   */
  public void print() {
    Iterator<String> iter = getKeys();
    while (iter.hasNext()){
      String key = iter.next();
      log.info(key + " = " + getProperty(key));
    }
  }
  
  private boolean contains(String key) {
    if (containsKey(key) == false) {
      log.info(key + " is not set");
      return false;
    }
    return true;
  }
  
  /**
   * Returns true if required properties for FluoClient are set
   */
  public boolean hasRequiredClientProps() {
    boolean valid = true;
    valid &= contains(CLIENT_ACCUMULO_USER_PROP);
    valid &= contains(CLIENT_ACCUMULO_PASSWORD_PROP);
    valid &= contains(CLIENT_ACCUMULO_INSTANCE_PROP);
    return valid;
  }
  
  /**
   * Returns true if required properties for FluoAdmin are set
   */
  public boolean hasRequiredAdminProps() {
    boolean valid = true;
    valid &= hasRequiredClientProps();
    valid &= contains(ADMIN_ACCUMULO_TABLE_PROP);
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
    valid &= hasRequiredClientProps();
    valid &= hasRequiredAdminProps();
    valid &= hasRequiredOracleProps();
    valid &= hasRequiredWorkerProps();
    return valid;
  }
  
  /**
   * Returns configuration with all Fluo properties set to their
   * default. NOTE - some properties do not have defaults and 
   * will not be set.
   */
  public static Configuration getDefaultConfiguration() {
    Configuration config = new CompositeConfiguration();
    setDefaultConfiguration(config);
    return config;
  }

  /**
   * Sets all Fluo properties to their default in the given
   * configuration.  NOTE - some properties do not have defaults
   * and will not be set. 
   */
  public static void setDefaultConfiguration(Configuration config) {
    config.setProperty(CLIENT_ZOOKEEPER_CONNECT_PROP, CLIENT_ZOOKEEPER_CONNECT_DEFAULT);
    config.setProperty(CLIENT_ZOOKEEPER_ROOT_PROP, CLIENT_ZOOKEEPER_ROOT_DEFAULT);
    config.setProperty(CLIENT_ZOOKEEPER_TIMEOUT_PROP, CLIENT_ZOOKEEPER_TIMEOUT_DEFAULT);
    config.setProperty(CLIENT_CLASS_PROP, CLIENT_CLASS_DEFAULT);
    config.setProperty(ADMIN_ZOOKEEPER_CLEAR_PROP, ADMIN_ZOOKEEPER_CLEAR_DEFAULT);
    config.setProperty(ADMIN_CLASS_PROP, ADMIN_CLASS_DEFAULT);
    config.setProperty(WORKER_NUM_THREADS_PROP, WORKER_NUM_THREADS_DEFAULT);
    config.setProperty(WORKER_INSTANCES_PROP, WORKER_INSTANCES_DEFAULT);
    config.setProperty(WORKER_MAX_MEMORY_MB_PROP, WORKER_MAX_MEMORY_MB_DEFAULT);
    config.setProperty(TRANSACTION_ROLLBACK_TIME_PROP, TRANSACTION_ROLLBACK_TIME_DEFAULT);
    config.setProperty(LOADER_NUM_THREADS_PROP, LOADER_NUM_THREADS_DEFAULT);
    config.setProperty(LOADER_QUEUE_SIZE_PROP, LOADER_QUEUE_SIZE_DEFAULT);
    config.setProperty(ORACLE_PORT_PROP, ORACLE_PORT_DEFAULT);
    config.setProperty(ORACLE_MAX_MEMORY_MB_PROP, ORACLE_MAX_MEMORY_MB_DEFAULT);
    config.setProperty(MINI_CLASS_PROP, MINI_CLASS_DEFAULT);
  }
}
