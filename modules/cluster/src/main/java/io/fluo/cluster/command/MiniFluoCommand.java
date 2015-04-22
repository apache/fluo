/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.cluster.command;

import java.io.File;
import java.util.Arrays;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.cluster.runner.MiniAppRunner;
import io.fluo.cluster.util.FluoPath;
import io.fluo.core.mini.MiniFluoImpl;
import org.slf4j.LoggerFactory;

public class MiniFluoCommand {

  public static void main(String[] args) {

    if (args.length < 3) {
      System.err
          .println("ERROR - Expected at least two arguments.  Usage: FluoCommand <fluoHomeDir> <command> <appName> ...");
      System.exit(-1);
    }

    String fluoHomeDir = args[0];
    String command = args[1];
    String appName = args[2];
    String[] remainArgs = Arrays.copyOfRange(args, 3, args.length);

    if (command.equalsIgnoreCase("scan")) {
      for (String logger : new String[] {Logger.ROOT_LOGGER_NAME, "io.fluo"}) {
        ((Logger) LoggerFactory.getLogger(logger)).setLevel(Level.ERROR);
      }
    }

    FluoPath cu = new FluoPath(fluoHomeDir, appName);

    MiniAppRunner runner = new MiniAppRunner(cu.getAppConfiguration());

    switch (command.toLowerCase()) {
      case "scan":
        FluoConfiguration config = runner.getConfiguration();
        FluoConfiguration chosenConfig = null;

        if (config.hasRequiredClientProps()) {
          chosenConfig = config;

        } else {
          File miniDataDir = new File(config.getMiniDataDir());
          if (!miniDataDir.exists()) {
            System.err
                .println("Cannot connect to Fluo '"
                    + config.getApplicationName()
                    + "' application!  Client properties are not set in fluo.properties and a MiniAccumuloCluster is not running at "
                    + miniDataDir.getAbsolutePath());
            System.exit(-1);
          }

          if (!config.hasRequiredMiniFluoProps()) {
            System.err.println("Fluo properties are not configured correctly!");
            System.exit(-1);
          }

          File clientPropsFile = new File(MiniFluoImpl.clientPropsPath(config));
          if (!clientPropsFile.exists()) {
            System.err.println("MiniFluo client.properties do not exist at "
                + clientPropsFile.getAbsolutePath());
            System.exit(-1);
          }
          chosenConfig = new FluoConfiguration(clientPropsFile);
        }
        runner.scan(chosenConfig, remainArgs);
        break;
      case "cleanup":
        runner.cleanup();
        break;
      default:
        System.err.println("Unknown command: " + command);
        break;
    }
  }
}
