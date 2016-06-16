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
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.fluo.core.worker.NotificationFinder;
import org.apache.fluo.core.worker.NotificationFinderFactory;
import org.apache.fluo.core.worker.NotificationProcessor;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.fluo.cluster.main.MainOptions.STDOUT;

/**
 * Main run method of Fluo worker that can be called within a Twill/YARN application or on its own
 * as a Java application
 */
public class FluoWorkerMain extends AbstractTwillRunnable {

  private static final Logger log = LoggerFactory.getLogger(FluoWorkerMain.class);
  public static String WORKER_NAME = "FluoWorker";
  private AtomicBoolean shutdown = new AtomicBoolean(false);

  @Override
  public void run() {
    System.out.println("Starting Worker");
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
        LogbackUtil.init("worker", options.getConfigDir(), options.getLogOutput());
      }
    } catch (Exception e) {
      System.err.println("Exception while starting FluoWorker: " + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }

    try {
      FluoConfiguration config = new FluoConfiguration(new File(options.getFluoProps()));
      if (!config.hasRequiredWorkerProps()) {
        log.error("fluo.properties is missing required properties for worker");
        System.exit(-1);
      }
      // any client in worker should retry forever
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
        System.setProperty(MetricNames.METRICS_ID_PROP, "worker-" + context.getInstanceId());
      }

      try (Environment env = new Environment(config);
          AutoCloseable reporters = ReporterUtil.setupReporters(env);
          NodeCache appIdCache = ClusterUtil.startAppIdWatcher(env)) {
        log.info("Starting Worker for Fluo '{}' application with the following configuration:",
            config.getApplicationName());
        env.getConfiguration().print();

        NotificationProcessor np = new NotificationProcessor(env);
        NotificationFinder notificationFinder =
            NotificationFinderFactory.newNotificationFinder(env.getConfiguration());
        notificationFinder.init(env, np);
        notificationFinder.start();

        while (!shutdown.get()) {
          UtilWaitThread.sleep(1000);
        }

        notificationFinder.stop();
      }
    } catch (Exception e) {
      log.error("Exception running FluoWorker: ", e);
    }

    log.info("Worker is exiting.");
  }

  @Override
  public void stop() {
    log.info("Stopping Fluo worker");
    shutdown.set(true);
  }

  public static void main(String[] args) throws Exception {
    FluoWorkerMain worker = new FluoWorkerMain();
    worker.run(args);
  }
}
