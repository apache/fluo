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
package io.fluo.cluster.util;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.fluo.api.config.FluoConfiguration.FLUO_PREFIX;

/**
 * Used to initialize Logging for cluster applications
 */
public class Log4jUtil {
  
  private static final Logger log = LoggerFactory.getLogger(Log4jUtil.class);
  private static final String LOG_APPLICATION_PROP = FLUO_PREFIX + ".log.application";
  private static final String LOG_DIR_PROP = FLUO_PREFIX + ".log.dir";
  private static final String LOG_LOCAL_HOSTNAME_PROP = FLUO_PREFIX + ".log.local.hostname";
  
  public static void init(String application, String configDir, String logOutput) throws IOException {
    init(application, configDir, logOutput, true);
  }

  public static void init(String application, String configDir, String logOutput, boolean debug) throws IOException {
    
    String logConfig;
    
    if (logOutput.equalsIgnoreCase("STDOUT")) {
      logConfig = String.format("%s/log4j-stdout.xml", configDir);
    } else {

      System.setProperty(LOG_APPLICATION_PROP, application);
      System.setProperty(LOG_DIR_PROP, logOutput);

      String localhost = InetAddress.getLocalHost().getHostName();
      System.setProperty(LOG_LOCAL_HOSTNAME_PROP, localhost);

      // Use a specific log config, if it exists
      logConfig = String.format("%s/log4j-file-%s.xml", configDir, application);
      if (!new File(logConfig).exists()) {
        // otherwise, use the generic config
        logConfig = String.format("%s/log4j-file.xml", configDir);
      }
    }
    
    ClusterUtil.verifyConfigPathsExist(logConfig);
        
    DOMConfigurator.configureAndWatch(logConfig, 5000);

    if (debug) {
      System.out.println("Logging to " + logOutput + " using config " + logConfig);
      log.info("Initialized logging using config in " + logConfig);
      log.info("Starting " + application + " application");
    }
  }
}
