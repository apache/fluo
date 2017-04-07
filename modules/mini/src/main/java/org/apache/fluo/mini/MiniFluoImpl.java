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

package org.apache.fluo.mini;

import java.io.File;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoAdmin.InitializationOptions;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.oracle.OracleServer;
import org.apache.fluo.core.worker.NotificationFinder;
import org.apache.fluo.core.worker.NotificationFinderFactory;
import org.apache.fluo.core.worker.NotificationProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of MiniFluo
 */
public class MiniFluoImpl implements MiniFluo {

  private static final Logger log = LoggerFactory.getLogger(MiniFluoImpl.class);

  private static final AtomicInteger reporterCounter = new AtomicInteger(1);

  private final Environment env;
  private OracleServer oserver;

  private NotificationProcessor mnp;
  private NotificationFinder notificationFinder;
  private FluoConfiguration config;
  private MiniAccumuloCluster cluster = null;

  protected static String USER = "root";
  protected static String PASSWORD = "secret";

  private AutoCloseable reporter;

  public static String clientPropsPath(FluoConfiguration config) {
    return config.getMiniDataDir() + "/client.properties";
  }

  @VisibleForTesting
  public NotificationProcessor getNotificationProcessor() {
    return mnp;
  }

  public MiniFluoImpl(FluoConfiguration fluoConfig) {
    if (!fluoConfig.hasRequiredMiniFluoProps()) {
      throw new IllegalArgumentException("MiniFluo configuration is not valid");
    }
    config = fluoConfig;

    try {
      if (config.getMiniStartAccumulo()) {
        startMiniAccumulo();
      }

      config.setProperty(FluoConfigurationImpl.NTFY_FINDER_MIN_SLEEP_TIME_PROP, 50);
      config.setProperty(FluoConfigurationImpl.NTFY_FINDER_MAX_SLEEP_TIME_PROP, 100);

      env = new Environment(config);

      reporter = FluoClientImpl.setupReporters(env, "mini", reporterCounter);

      oserver = new OracleServer(env);
      oserver.start();

      mnp = new NotificationProcessor(env);
      notificationFinder = NotificationFinderFactory.newNotificationFinder(env.getConfiguration());
      notificationFinder.init(env, mnp);
      notificationFinder.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void startMiniAccumulo() {
    try {
      // start mini accumulo cluster
      MiniAccumuloConfig cfg = new MiniAccumuloConfig(new File(config.getMiniDataDir()), PASSWORD);
      cluster = new MiniAccumuloCluster(cfg);
      cluster.start();

      log.debug("Started MiniAccumulo(accumulo=" + cluster.getInstanceName() + " zk="
          + cluster.getZooKeepers() + ")");

      // configuration that must overridden
      config.setAccumuloInstance(cluster.getInstanceName());
      config.setAccumuloUser(USER);
      config.setAccumuloPassword(PASSWORD);
      config.setAccumuloZookeepers(cluster.getZooKeepers());
      config.setInstanceZookeepers(cluster.getZooKeepers() + "/fluo");

      // configuration that only needs to be set if not by user
      if ((config.containsKey(FluoConfiguration.ADMIN_ACCUMULO_TABLE_PROP) == false)
          || config.getAccumuloTable().trim().isEmpty()) {
        config.setAccumuloTable("fluo");
      }

      InitializationOptions opts = new InitializationOptions();
      try (FluoAdmin admin = FluoFactory.newAdmin(config)) {
        admin.initialize(opts);
      }

      File miniProps = new File(clientPropsPath(config));
      config.getClientConfiguration().save(miniProps);

      log.debug("Wrote MiniFluo client properties to {}", miniProps.getAbsolutePath());

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public SimpleConfiguration getClientConfiguration() {
    return config.getClientConfiguration();
  }

  @Override
  public void close() {
    try {
      if (oserver != null) {
        notificationFinder.stop();
        mnp.close();
        oserver.stop();
        env.close();
        reporter.close();
        if (cluster != null) {
          cluster.stop();
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void waitForObservers() {
    try {
      Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
      Notification.configureScanner(scanner);

      while (true) {
        long ts1 = env.getSharedResources().getOracleClient().getStamp().getTxTimestamp();
        long ntfyCount = Iterables.size(scanner);
        long ts2 = env.getSharedResources().getOracleClient().getStamp().getTxTimestamp();
        if (ntfyCount == 0 && ts1 == (ts2 - 1)) {
          break;
        }

        long sleepTime = ntfyCount / 2;
        sleepTime = Math.min(Math.max(10, sleepTime), 10000);
        Uninterruptibles.sleepUninterruptibly(sleepTime, TimeUnit.MILLISECONDS);
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
