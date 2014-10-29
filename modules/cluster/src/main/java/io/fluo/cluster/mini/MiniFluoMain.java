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
package io.fluo.cluster.mini;

import java.io.File;

import com.beust.jcommander.JCommander;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.mini.MiniFluo;
import io.fluo.cluster.util.Log4jUtil;
import io.fluo.cluster.util.MainOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main method for MiniFluo
 */
public class MiniFluoMain {
  
  private static final Logger log = LoggerFactory.getLogger(MiniFluoMain.class);

  public static void main(String[] args) {

    MainOptions options = new MainOptions();
    try {
      JCommander jcommand = new JCommander(options, args);

      if (options.help) {
        jcommand.usage();
        System.exit(-1);
      }
      options.validateConfig();

      Log4jUtil.init("mini", options.getConfigDir(), options.getLogOutput());
    } catch (Exception e) {
      System.err.println("Exception running MiniFluo: "+ e.getMessage());
      e.printStackTrace();
    }

    try {
      FluoConfiguration config = new FluoConfiguration(new File(options.getFluoProps()));
      if (!config.hasRequiredMiniFluoProps()) {
        log.error("fluo.properties is missing required properties for MiniFluo");
        System.exit(-1);
      }
      try (MiniFluo mini = FluoFactory.newMiniFluo(config)) {
        log.info("MiniFluo is running");
                
        while (true) {
          Thread.sleep(100);
        }
      }
    } catch (Exception e) {
      log.error("Exception running MiniFluo: ", e);
    }
    
    log.info("MiniFluo is exiting.");
  }
}
