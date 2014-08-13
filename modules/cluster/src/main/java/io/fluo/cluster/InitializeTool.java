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
package io.fluo.cluster;

import java.io.File;

import com.beust.jcommander.JCommander;

import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoAdmin.AlreadyInitializedException;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.cluster.util.Logging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * Initializes Fluo using properties in configuration files
 */
public class InitializeTool {
  
  private static Logger log = LoggerFactory.getLogger(InitializeTool.class);

  public static void main(String[] args) throws Exception {

    RunnableOptions options = new RunnableOptions();
    JCommander jcommand = new JCommander(options, args);

    if (options.help) {
      jcommand.usage();
      System.exit(-1);
    }
    options.validateConfig();

    Logging.init("init", options.getConfigDir(), options.getLogOutput());

    FluoConfiguration config = new FluoConfiguration(new File(options.getFluoProps()));
    if (!config.hasRequiredAdminProps()) {
      log.error("fluo.properties is missing required properties for initialization");
      System.exit(-1);
    }

    FluoAdmin admin = FluoFactory.newAdmin(config);
    try {
      admin.initialize();
    } catch (AlreadyInitializedException aie) {
      admin.updateSharedConfig();
    }
  }
}
