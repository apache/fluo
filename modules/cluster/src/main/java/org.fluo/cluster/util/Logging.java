/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fluo.cluster.util;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

/**
 * 
 */
public class Logging {
  
  private static Logger log = LoggerFactory.getLogger(Logging.class);
  
  public static void init(String application, String configDir, String logOutput) throws IOException {
    
    String logConfig;
    
    if (logOutput.equalsIgnoreCase("STDOUT")) {
      // Use a specific log config, if it exists
      logConfig = String.format("%s/logback-stdout-%s.xml", configDir, application);
      if (!new File(logConfig).exists()) {
        // otherwise, use the generic config
        logConfig = String.format("%s/logback-stdout.xml", configDir);
      }
    } else {
      
      System.setProperty("org.fluo.log.application", application);
      System.setProperty("org.fluo.log.dir.log", logOutput);

      String localhost = InetAddress.getLocalHost().getHostName();
      System.setProperty("org.fluo.log.ip.localhost.hostname", localhost);
 
      // Use a specific log config, if it exists
      logConfig = String.format("%s/logback-file-%s.xml", configDir, application);
      if (!new File(logConfig).exists()) {
        // otherwise, use the generic config
        logConfig = String.format("%s/logback-file.xml", configDir);
      }
    } 
        
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

    System.out.println("Logging to "+logOutput+" using config "+ logConfig);
    log.info("Initialized logging using config in "+ logConfig);
    
    log.info("Starting "+application+" application");
    
    // TODO print info about instance like zookeepers, zookeeper root
  }
}
