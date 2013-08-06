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
package org.apache.accumulo.accismus.impl;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * 
 */
public class Logging {
  
  private static Logger log = Logger.getLogger(Logging.class);
  
  public static void init(String application) throws UnknownHostException {
    
    System.setProperty("accismus.log.application", application);
    
    if (System.getenv("ACCISMUS_LOG_DIR") != null)
      System.setProperty("accismus.log.dir.log", System.getenv("ACCISMUS_LOG_DIR"));
    else
      System.setProperty("accismus.log.dir.log", System.getenv("ACCISMUS_HOME") + "/logs/");
    
    String localhost = InetAddress.getLocalHost().getHostName();
    System.setProperty("accismus.log.ip.localhost.hostname", localhost);
    
    if (System.getenv("ACCISMUS_LOG_HOST") != null)
      System.setProperty("accismus.log.host.log", System.getenv("ACCISMUS_LOG_HOST"));
    else
      System.setProperty("accismus.log.host.log", localhost);
    
    // Use a specific log config, if it exists
    String logConfig = String.format("%s/%s_logger.xml", System.getenv("ACCISMUS_CONF_DIR"), application);
    if (!new File(logConfig).exists()) {
      // otherwise, use the generic config
      logConfig = String.format("%s/generic_logger.xml", System.getenv("ACCISMUS_CONF_DIR"));
    }
    
    // Configure logging
    DOMConfigurator.configureAndWatch(logConfig, 5000);
    
    log.info(application + " starting");
    // TODO print info about instance like zookeepers, zookeeper root
  }
}
