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

import java.util.Arrays;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.cluster.runner.YarnAppRunner;
import org.apache.fluo.cluster.util.FluoInstall;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Fluo command
 */
@Deprecated
public class FluoCommand {

  public static void verifyNoArgs(String[] remainArgs) {
    if (remainArgs.length != 0) {
      System.err.println(
          "ERROR - Received unexpected command-line arguments: " + Arrays.toString(remainArgs));
      System.exit(-1);
    }
  }

  public static void main(String[] args) {

    if (args.length < 4) {
      System.err.println("ERROR - Expected at least two arguments.  "
          + "Usage: FluoCommand <fluoHomeDir> <hadoopPrefix> <command> <appName> ...");
      System.exit(-1);
    }

    String fluoHomeDir = args[0];
    String hadoopPrefix = args[1];
    String command = args[2];
    String appName = args[3];
    String[] remainArgs = Arrays.copyOfRange(args, 4, args.length);

    if (command.equalsIgnoreCase("scan")) {
      for (String logger : new String[] {Logger.ROOT_LOGGER_NAME, "org.apache.fluo"}) {
        ((Logger) LoggerFactory.getLogger(logger)).setLevel(Level.ERROR);
      }
    }

    FluoInstall fluoInstall = new FluoInstall(fluoHomeDir);

    try (YarnAppRunner runner = new YarnAppRunner(hadoopPrefix)) {
      switch (command.toLowerCase()) {
        case "init":
          runner.init(fluoInstall.getAppConfiguration(appName),
              fluoInstall.getAppPropsPath(appName), remainArgs);
          break;
        case "list":
          verifyNoArgs(remainArgs);
          runner.list(fluoInstall.getFluoConfiguration());
          break;
        case "start":
          verifyNoArgs(remainArgs);
          runner.start(fluoInstall.getAppConfiguration(appName), fluoInstall.getAppConfDir(appName),
              fluoInstall.getAppLibDir(appName), fluoInstall.getLibDir());
          break;
        case "scan":
          runner.scan(fluoInstall.resolveFluoConfiguration(appName), remainArgs);
          break;
        case "stop":
          verifyNoArgs(remainArgs);
          runner.stop(fluoInstall.resolveFluoConfiguration(appName));
          break;
        case "kill":
          verifyNoArgs(remainArgs);
          runner.kill(fluoInstall.resolveFluoConfiguration(appName));
          break;
        case "status":
          verifyNoArgs(remainArgs);
          runner.status(fluoInstall.resolveFluoConfiguration(appName), false);
          break;
        case "info":
          verifyNoArgs(remainArgs);
          runner.status(fluoInstall.resolveFluoConfiguration(appName), true);
          break;
        case "wait":
          verifyNoArgs(remainArgs);
          runner.waitUntilFinished(fluoInstall.resolveFluoConfiguration(appName));
          break;
        case "exec":
          runner.exec(fluoInstall.resolveFluoConfiguration(appName, false), remainArgs);
          break;
        default:
          System.err.println("Unknown command: " + command);
          break;
      }
    } catch (FluoException e) {
      System.err.println("ERROR - " + e.getMessage());
      System.exit(-1);
    } catch (Exception e) {
      System.err.println("Command failed due to exception below:");
      e.printStackTrace();
      System.exit(-1);
    }

    // TODO FLUO-464 - Speed up exit and remove System.exit() below
    System.exit(0);
  }
}
