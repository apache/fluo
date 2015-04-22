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

import java.util.Arrays;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.fluo.cluster.runner.AppRunner;
import io.fluo.cluster.runner.YarnAppRunner;
import io.fluo.cluster.util.FluoPath;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Fluo command
 */
public class FluoCommand {

  public static void main(String[] args) {

    if (args.length < 4) {
      System.err
          .println("ERROR - Expected at least two arguments.  Usage: FluoCommand <fluoHomeDir> <hadoopPrefix> <command> <appName> ...");
      System.exit(-1);
    }

    String fluoHomeDir = args[0];
    String hadoopPrefix = args[1];
    String command = args[2];
    String appName = args[3];
    String[] remainArgs = Arrays.copyOfRange(args, 4, args.length);

    FluoPath fluoPath = new FluoPath(fluoHomeDir, appName);

    if (command.equalsIgnoreCase("scan")) {
      for (String logger : new String[] {Logger.ROOT_LOGGER_NAME, "io.fluo"}) {
        ((Logger) LoggerFactory.getLogger(logger)).setLevel(Level.ERROR);
      }
    } else if (command.equalsIgnoreCase("classpath")) {
      AppRunner.classpath("fluo", fluoHomeDir, remainArgs);
      return;
    }

    try (YarnAppRunner runner =
        new YarnAppRunner(fluoPath.getAppConfiguration(), fluoPath, hadoopPrefix)) {
      switch (command.toLowerCase()) {
        case "init":
          runner.init(remainArgs);
          break;
        case "start":
          runner.start();
          break;
        case "scan":
          runner.scan(remainArgs);
          break;
        case "stop":
          runner.stop();
          break;
        case "kill":
          runner.kill();
          break;
        case "status":
          runner.status(false);
          break;
        case "info":
          runner.status(true);
          break;
        case "wait":
          runner.waitUntilFinished();
          break;
        default:
          System.err.println("Unknown command: " + command);
          break;
      }
    } catch (Exception e) {
      System.err.println("Command failed due to exception below:");
      e.printStackTrace();
      System.exit(-1);
    }

    // TODO FLUO-464 - Speed up exit and remove System.exit() below
    System.exit(0);
  }
}
