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

package org.apache.fluo.cluster.util;

import java.io.IOException;
import java.net.InetAddress;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.fluo.api.config.FluoConfiguration.FLUO_PREFIX;

/**
 * Used to initialize Logging for cluster applications
 */
public class LogbackUtil {

  private static final Logger log = LoggerFactory.getLogger(LogbackUtil.class);

  private static final String FLUO_LOG_APP = FLUO_PREFIX + ".log.app";
  private static final String FLUO_LOG_DIR = FLUO_PREFIX + ".log.dir";
  private static final String FLUO_LOG_HOST = FLUO_PREFIX + ".log.host";

  public static void init(String application, String configDir, String logDir) throws IOException {

    String logConfig = String.format("%s/logback.xml", configDir);
    ClusterUtil.verifyConfigPathsExist(logConfig);

    System.setProperty(FLUO_LOG_APP, application);
    System.setProperty(FLUO_LOG_DIR, logDir);

    String localHostname = InetAddress.getLocalHost().getHostName();
    String instanceId = System.getenv("TWILL_INSTANCE_ID");
    String logHost = localHostname;
    if (instanceId != null) {
      logHost = String.format("%s_%s", instanceId, localHostname);
    }
    System.setProperty(FLUO_LOG_HOST, logHost);

    // assume SLF4J is bound to logback in the current environment
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

    try {
      JoranConfigurator configurator = new JoranConfigurator();
      configurator.setContext(context);
      // Call context.reset() to clear any previous configuration, e.g. default
      // configuration. For multi-step configuration, omit calling context.reset().
      context.reset();
      configurator.doConfigure(logConfig);
    } catch (JoranException je) {
      // StatusPrinter will handle this
    }
    StatusPrinter.printInCaseOfErrorsOrWarnings(context);

    System.out.println("Logging to " + logDir + " using config " + logConfig);
    log.info("Initialized logging using config in " + logConfig);
  }
}
