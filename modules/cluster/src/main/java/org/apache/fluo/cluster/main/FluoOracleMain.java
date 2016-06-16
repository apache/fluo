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

package org.apache.fluo.cluster.main;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import com.beust.jcommander.JCommander;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.cluster.util.ClusterUtil;
import org.apache.fluo.cluster.util.LogbackUtil;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.metrics.MetricNames;
import org.apache.fluo.core.metrics.ReporterUtil;
import org.apache.fluo.core.oracle.OracleServer;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.fluo.cluster.main.MainOptions.STDOUT;

/**
 * Main method of Fluo oracle that can be called within a Twill/YARN application or on its own as a
 * Java application
 */
public class FluoOracleMain extends AbstractTwillRunnable {

  private static final Logger log = LoggerFactory.getLogger(FluoOracleMain.class);
  public static String ORACLE_NAME = "FluoOracle";
  private AtomicBoolean shutdown = new AtomicBoolean(false);

  @Override
  public void run() {
    System.out.println("Starting Oracle");
    String logDir = System.getenv("LOG_DIRS");
    if (logDir == null) {
      System.err
          .println("LOG_DIRS env variable was not set by Twill.  Logging to console instead!");
      logDir = STDOUT;
    }
    run(new String[] {"-config-dir", "./conf", "-log-output", logDir});
  }

  public void run(String[] args) {
    MainOptions options = new MainOptions();
    try {
      JCommander jcommand = new JCommander(options, args);

      if (options.help) {
        jcommand.usage();
        System.exit(-1);
      }
      options.validateConfig();

      if (!options.getLogOutput().equals(STDOUT)) {
        LogbackUtil.init("oracle", options.getConfigDir(), options.getLogOutput());
      }
    } catch (Exception e) {
      System.err.println("Exception while starting FluoOracle: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }

    try {
      FluoConfiguration config = new FluoConfiguration(new File(options.getFluoProps()));
      if (!config.hasRequiredOracleProps()) {
        log.error("fluo.properties is missing required properties for oracle");
        System.exit(-1);
      }
      // any client in oracle should retry forever
      config.setClientRetryTimeout(-1);

      try {
        config.validate();
      } catch (Exception e) {
        System.err.println("Error - Invalid fluo.properties due to " + e.getMessage());
        e.printStackTrace();
        System.exit(-1);
      }

      TwillContext context = getContext();
      if (context != null && System.getProperty(MetricNames.METRICS_ID_PROP) == null) {
        System.setProperty(MetricNames.METRICS_ID_PROP, "oracle-" + context.getInstanceId());
      }

      try (Environment env = new Environment(config);
          AutoCloseable reporters = ReporterUtil.setupReporters(env);
          NodeCache appIdCache = ClusterUtil.startAppIdWatcher(env)) {
        log.info("Starting Oracle for Fluo '{}' application with the following configuration:",
            config.getApplicationName());
        env.getConfiguration().print();

        OracleServer server = new OracleServer(env);
        server.start();

        while (!shutdown.get()) {
          UtilWaitThread.sleep(10000);
        }

        server.stop();
      }

    } catch (Exception e) {
      log.error("Exception running FluoOracle: ", e);
    }

    log.info("FluoOracle is exiting.");
  }

  @Override
  public void stop() {
    log.info("Stopping Fluo oracle");
    shutdown.set(true);
  }

  public static void main(String[] args) {
    FluoOracleMain oracle = new FluoOracleMain();
    oracle.run(args);
  }
}
