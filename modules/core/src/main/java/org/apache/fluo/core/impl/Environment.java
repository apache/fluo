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

package org.apache.fluo.core.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import com.google.common.annotations.VisibleForTesting;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.fluo.core.metrics.MetricNames;
import org.apache.fluo.core.metrics.MetricsReporterImpl;
import org.apache.fluo.core.observer.RegisteredObservers;
import org.apache.fluo.core.observer.ObserverUtil;
import org.apache.fluo.core.util.AccumuloUtil;
import org.apache.fluo.core.util.CuratorUtil;

/**
 * Holds common environment configuration and shared resources
 */
public class Environment implements AutoCloseable {

  private String table;
  private Authorizations auths = new Authorizations();
  private String accumuloInstance;
  private RegisteredObservers observers;
  private Connector conn;
  private String accumuloInstanceID;
  private String fluoApplicationID;
  private FluoConfiguration config;
  private SharedResources resources;
  private MetricNames metricNames;
  private SimpleConfiguration appConfig;
  private String metricsReporterID;

  /**
   * Constructs an environment from given FluoConfiguration
   *
   * @param configuration Configuration used to configure environment
   */
  public Environment(FluoConfiguration configuration) {
    config = configuration;
    conn = AccumuloUtil.getConnector(config);

    readZookeeperConfig();

    if (!conn.getInstance().getInstanceName().equals(accumuloInstance)) {
      throw new IllegalArgumentException("unexpected accumulo instance name "
          + conn.getInstance().getInstanceName() + " != " + accumuloInstance);
    }

    if (!conn.getInstance().getInstanceID().equals(accumuloInstanceID)) {
      throw new IllegalArgumentException("unexpected accumulo instance id "
          + conn.getInstance().getInstanceID() + " != " + accumuloInstanceID);
    }

    try {
      resources = new SharedResources(this);
    } catch (TableNotFoundException e1) {
      throw new IllegalStateException(e1);
    }
  }

  /**
   * Constructs an environment from another environment
   *
   * @param env Environment
   */
  @VisibleForTesting
  public Environment(Environment env) throws Exception {
    this.table = env.table;
    this.auths = env.auths;
    this.accumuloInstance = env.accumuloInstance;
    this.observers = env.observers;
    this.conn = env.conn;
    this.accumuloInstanceID = env.accumuloInstanceID;
    this.fluoApplicationID = env.fluoApplicationID;
    this.config = env.config;
    this.resources = new SharedResources(this);
  }

  /**
   * Read configuration from zookeeper
   */
  private void readZookeeperConfig() {

    try (CuratorFramework curator = CuratorUtil.newAppCurator(config)) {
      curator.start();

      accumuloInstance =
          new String(curator.getData().forPath(ZookeeperPath.CONFIG_ACCUMULO_INSTANCE_NAME),
              StandardCharsets.UTF_8);
      accumuloInstanceID =
          new String(curator.getData().forPath(ZookeeperPath.CONFIG_ACCUMULO_INSTANCE_ID),
              StandardCharsets.UTF_8);
      fluoApplicationID =
          new String(curator.getData().forPath(ZookeeperPath.CONFIG_FLUO_APPLICATION_ID),
              StandardCharsets.UTF_8);

      table =
          new String(curator.getData().forPath(ZookeeperPath.CONFIG_ACCUMULO_TABLE),
              StandardCharsets.UTF_8);

      observers = ObserverUtil.load(curator);

      config = FluoAdminImpl.mergeZookeeperConfig(config);

      // make sure not to include config passed to env, only want config from zookeeper
      appConfig = config.getAppConfiguration();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
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

  public String getFluoApplicationID() {
    return fluoApplicationID;
  }

  public RegisteredObservers getConfiguredObservers() {
    return observers;
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

  public synchronized String getMetricsReporterID() {
    if (metricsReporterID == null) {
      String mid = System.getProperty(MetricNames.METRICS_REPORTER_ID_PROP);
      if (mid == null) {
        try {
          String hostname = InetAddress.getLocalHost().getHostName();
          int idx = hostname.indexOf('.');
          if (idx > 0) {
            hostname = hostname.substring(0, idx);
          }
          mid = hostname + "_" + getSharedResources().getTransactorID();
        } catch (UnknownHostException e) {
          throw new RuntimeException(e);
        }
      }
      metricsReporterID = mid.replace('.', '_');
    }
    return metricsReporterID;
  }

  public String getMetricsAppName() {
    return config.getApplicationName().replace('.', '_');
  }

  public synchronized MetricNames getMetricNames() {
    if (metricNames == null) {
      metricNames = new MetricNames(getMetricsReporterID(), getMetricsAppName());
    }
    return metricNames;
  }

  public MetricsReporter getMetricsReporter() {
    return new MetricsReporterImpl(getConfiguration(), getSharedResources().getMetricRegistry(),
        getMetricsReporterID());
  }

  public SimpleConfiguration getAppConfiguration() {
    // TODO create immutable wrapper
    return new SimpleConfiguration(appConfig);
  }

  @Override
  public void close() {
    resources.close();
  }
}
