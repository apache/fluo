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

package org.apache.fluo.cluster.command;

import java.io.File;
import java.util.Arrays;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.cluster.runner.MiniAppRunner;
import org.apache.fluo.cluster.util.FluoInstall;
import org.apache.fluo.mini.MiniFluoImpl;
import org.slf4j.LoggerFactory;

public class MiniFluoCommand {

  static FluoInstall fluoInstall;

  public static void verifyNoArgs(String[] remainArgs) {
    if (remainArgs.length != 0) {
      System.err.println("ERROR - Received unexpected command-line arguments: "
          + Arrays.toString(remainArgs));
      System.exit(-1);
    }
  }

  public static FluoConfiguration chooseConfig(String appName) {

    FluoConfiguration chosenConfig = null;
    String propsPath;
    FluoConfiguration fluoConfig = fluoInstall.resolveFluoConfiguration(appName, false);
    if (fluoConfig.hasRequiredClientProps()) {
      chosenConfig = fluoConfig;
      propsPath = fluoInstall.getFluoPropsPath();
    } else {
      File miniDataDir = new File(fluoConfig.getMiniDataDir());
      if (!miniDataDir.exists()) {
        System.err.println("Cannot connect to Fluo '" + fluoConfig.getApplicationName()
            + "' application!  Client properties are not set in fluo.properties and "
            + " a MiniAccumuloCluster is not running at " + miniDataDir.getAbsolutePath());
        System.exit(-1);
      }
      if (!fluoConfig.hasRequiredMiniFluoProps()) {
        System.err.println("Fluo properties are not configured correctly!");
        System.exit(-1);
      }
      propsPath = MiniFluoImpl.clientPropsPath(fluoConfig);
      File clientPropsFile = new File(propsPath);
      if (!clientPropsFile.exists()) {
        System.err.println("MiniFluo client.properties do not exist at "
            + clientPropsFile.getAbsolutePath());
        System.exit(-1);
      }
      chosenConfig = new FluoConfiguration(clientPropsFile);
    }

    System.out.println("Connecting to MiniFluo instance (" + chosenConfig.getInstanceZookeepers()
        + ") using config (" + fluoInstall.stripFluoHomeDir(propsPath) + ")");
    return chosenConfig;
  }

  public static void main(String[] args) throws Exception {

    if (args.length < 3) {
      System.err.println("ERROR - Expected at least two arguments.  "
          + "Usage: MiniFluoCommand <fluoHomeDir> <command> <appName> ...");
      System.exit(-1);
    }

    String fluoHomeDir = args[0];
    String command = args[1];
    String appName = args[2];
    String[] remainArgs = Arrays.copyOfRange(args, 3, args.length);

    if (command.equalsIgnoreCase("scan")) {
      for (String logger : new String[] {Logger.ROOT_LOGGER_NAME, "org.apache.fluo"}) {
        ((Logger) LoggerFactory.getLogger(logger)).setLevel(Level.ERROR);
      }
    } else if (command.equalsIgnoreCase("wait")) {
      ((Logger) LoggerFactory.getLogger(FluoConfiguration.class)).setLevel(Level.ERROR);
    }

    fluoInstall = new FluoInstall(fluoHomeDir);
    MiniAppRunner runner = new MiniAppRunner();

    switch (command.toLowerCase()) {
      case "classpath":
        runner.classpath(fluoHomeDir, remainArgs);
        break;
      case "scan":
        runner.scan(chooseConfig(appName), remainArgs);
        break;
      case "cleanup":
        verifyNoArgs(remainArgs);
        runner.cleanup(fluoInstall.resolveFluoConfiguration(appName, false));
        break;
      case "wait":
        verifyNoArgs(remainArgs);
        runner.waitUntilFinished(chooseConfig(appName));
        break;
      case "exec":
        runner.exec(chooseConfig(appName), remainArgs);
        break;
      default:
        System.err.println("Unknown command: " + command);
        break;
    }
  }
}
