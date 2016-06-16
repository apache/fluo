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

import com.beust.jcommander.JCommander;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.core.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main method for MiniFluo
 */
public class MiniFluoMain {

  private static final Logger log = LoggerFactory.getLogger(MiniFluoMain.class);

  public static void main(String[] args) {

    try {
      MainOptions options = new MainOptions();
      JCommander jcommand = new JCommander(options, args);

      if (options.help) {
        jcommand.usage();
        System.exit(-1);
      }
      options.validateConfig();

      FluoConfiguration config = new FluoConfiguration(new File(options.getFluoProps()));
      if (!config.hasRequiredMiniFluoProps()) {
        log.error("Failed to start MiniFluo - fluo.properties is missing required properties for "
            + "MiniFluo");
        System.exit(-1);
      }
      try (MiniFluo mini = FluoFactory.newMiniFluo(config)) {
        log.info("MiniFluo is running");

        while (true) {
          UtilWaitThread.sleep(1000);
        }
      }
    } catch (Exception e) {
      log.error("Exception running MiniFluo: ", e);
    }

    log.info("MiniFluo is exiting.");
  }
}
