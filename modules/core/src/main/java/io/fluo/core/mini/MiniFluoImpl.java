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
package io.fluo.core.mini;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.mini.MiniFluo;
import io.fluo.core.client.FluoClientImpl;
import io.fluo.core.impl.Environment;
import io.fluo.core.oracle.OracleServer;
import io.fluo.core.util.ByteUtil;
import io.fluo.core.worker.NotificationFinder;
import io.fluo.core.worker.NotificationFinderFactory;
import io.fluo.core.worker.NotificationProcessor;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
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

  private int numProcessing = 0;
  private MiniNotificationProcessor mnp;
  private NotificationFinder notificationFinder;
  private FluoConfiguration config;
  private MiniAccumuloCluster cluster = null;
  
  protected static String USER = "root";
  protected static String PASSWORD = "secret";

  private AutoCloseable reporter;

  private class MiniNotificationProcessor extends NotificationProcessor {

    public MiniNotificationProcessor(Environment env) {
      super(env);
    }
    
    @Override
    protected void workAdded(){
      synchronized (MiniFluoImpl.this) {
        numProcessing++;
      }
    }
    
    @Override
    protected void workFinished(){
      synchronized (MiniFluoImpl.this) {
        numProcessing--;
      }
    }
  }
 
  @VisibleForTesting
  public NotificationProcessor getNotificationProcessor() {
    return mnp;
  }
  
  private synchronized boolean isProcessing(Scanner scanner) {
    return scanner.iterator().hasNext() || numProcessing > 0;
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
      env = new Environment(config);

      reporter = FluoClientImpl.setupReporters(env, "mini", reporterCounter);

      oserver = new OracleServer(env);
      oserver.start();

      mnp = new MiniNotificationProcessor(env);
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
      
      log.debug("Started MiniAccumulo(accumulo="+cluster.getInstanceName()+" zk="+cluster.getZooKeepers()+")");
      
      // configuration that must overridden
      config.setAccumuloInstance(cluster.getInstanceName());
      config.setAccumuloUser(USER);
      config.setAccumuloPassword(PASSWORD);
      config.setAccumuloZookeepers(cluster.getZooKeepers());
      config.setZookeepers(cluster.getZooKeepers()+"/fluo");
      
      // configuration that only needs to be set if not by user
      if ((config.containsKey(FluoConfiguration.ADMIN_ACCUMULO_TABLE_PROP) == false) ||
          config.getAccumuloTable().trim().isEmpty()) {
        config.setAccumuloTable("fluo");
      }

      FluoFactory.newAdmin(config).initialize();
      
      File miniProps = new File(config.getMiniDataDir()+"/client.properties");
      PropertiesConfiguration connConfig = new PropertiesConfiguration();
      connConfig.append(config.getClientConfiguration());
      connConfig.save(miniProps);
      
      log.debug("Wrote MiniFluo client properties to {}", miniProps.getAbsolutePath());
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public Configuration getClientConfiguration() {
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
      scanner.fetchColumnFamily(ByteUtil.toText(ColumnConstants.NOTIFY_CF));

      while (isProcessing(scanner)) { 
        Thread.sleep(100);
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
