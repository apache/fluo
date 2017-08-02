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

package org.apache.fluo.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Objects;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Preconditions;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;

public class FluoInit {

  public static class InitOptions {

    @Parameter(names = {"-f", "--force"},
        description = "Skip all prompts and clears Zookeeper and Accumulo table.  Equivalent to "
            + "setting both --clearTable --clearZookeeper")
    private boolean force;

    @Parameter(names = {"--clearTable"}, description = "Skips prompt and clears Accumulo table")
    private boolean clearTable;

    @Parameter(names = {"--clearZookeeper"}, description = "Skips prompt and clears Zookeeper")
    private boolean clearZookeeper;

    @Parameter(names = {"-u", "--update"}, description = "Update Fluo configuration in Zookeeper")
    private boolean update;

    @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
    boolean help;

    boolean getForce() {
      return force;
    }

    boolean getClearTable() {
      return clearTable;
    }

    boolean getClearZookeeper() {
      return clearZookeeper;
    }

    boolean getUpdate() {
      return update;
    }
  }

  private static boolean readYes() {
    String input = "unk";
    while (true) {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
      try {
        input = bufferedReader.readLine().trim();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
      if (input.equalsIgnoreCase("y")) {
        return true;
      } else if (input.equalsIgnoreCase("n")) {
        return false;
      } else {
        System.out.print("Unexpected input '" + input + "'. Enter y/n or ctrl-c to abort: ");
      }
    }
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err
          .println("Usage: FluoInit <connectionPropsPath> <appName> <applicationPropsPath> userArgs...");
      System.exit(-1);
    }
    String connectionPropsPath = args[0];
    Objects.requireNonNull(connectionPropsPath);
    Preconditions.checkArgument(!connectionPropsPath.isEmpty(), "<connectionPropsPath> is empty");
    File connectionPropsFile = new File(connectionPropsPath);
    Preconditions.checkArgument(connectionPropsFile.exists(), connectionPropsPath
        + " does not exist");

    String[] userArgs = Arrays.copyOfRange(args, 3, args.length);

    InitOptions commandOpts = new InitOptions();
    JCommander jcommand = new JCommander(commandOpts);
    jcommand.setProgramName("fluo init <app> <appProps>");
    try {
      jcommand.parse(userArgs);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      jcommand.usage();
      System.exit(-1);
    }

    if (commandOpts.help) {
      jcommand.usage();
      System.exit(1);
    }

    String applicationName = args[1];
    Objects.requireNonNull(applicationName);
    String applicationPropsPath = args[2];
    Objects.requireNonNull(applicationPropsPath);
    File applicationPropsFile = new File(applicationPropsPath);
    Preconditions.checkArgument(applicationPropsFile.exists(), applicationPropsPath
        + " does not exist");

    FluoConfiguration config = new FluoConfiguration(connectionPropsFile);
    config.load(applicationPropsFile);
    config.setApplicationName(applicationName);

    if (!config.hasRequiredAdminProps()) {
      System.err.println("Error - Required properties are not set in " + applicationPropsPath);
      System.exit(-1);
    }
    try {
      config.validate();
    } catch (Exception e) {
      System.err.println("Error - Invalid configuration due to " + e.getMessage());
      System.exit(-1);
    }

    try (FluoAdminImpl admin = new FluoAdminImpl(config)) {

      if (admin.oracleExists()) {
        System.err.println("Error - The Fluo '" + config.getApplicationName() + "' application"
            + " is already running and must be stopped before running 'fluo init'. "
            + " Aborted initialization.");
        System.exit(-1);
      }

      FluoAdmin.InitializationOptions initOpts = new FluoAdmin.InitializationOptions();

      if (commandOpts.getUpdate()) {
        System.out.println("Updating configuration for the Fluo '" + config.getApplicationName()
            + "' application in Zookeeper using " + applicationPropsPath);
        admin.updateSharedConfig();
        System.out.println("Update is complete.");
        System.exit(0);
      }

      if (commandOpts.getForce()) {
        initOpts.setClearZookeeper(true).setClearTable(true);
      } else {
        if (commandOpts.getClearZookeeper()) {
          initOpts.setClearZookeeper(true);
        } else if (admin.zookeeperInitialized()) {
          System.out.print("A Fluo '" + config.getApplicationName()
              + "' application is already initialized in Zookeeper at " + config.getAppZookeepers()
              + " - Would you like to clear and reinitialize Zookeeper"
              + " for this application (y/n)? ");
          if (readYes()) {
            initOpts.setClearZookeeper(true);
          } else {
            System.out.println("Aborted initialization.");
            System.exit(-1);
          }
        }

        if (commandOpts.getClearTable()) {
          initOpts.setClearTable(true);
        } else if (admin.accumuloTableExists()) {
          System.out.print("The Accumulo table '" + config.getAccumuloTable()
              + "' already exists - Would you like to drop and recreate this table (y/n)? ");
          if (readYes()) {
            initOpts.setClearTable(true);
          } else {
            System.out.println("Aborted initialization.");
            System.exit(-1);
          }
        }
      }

      System.out.println("Initializing Fluo '" + config.getApplicationName()
          + "' application using " + applicationPropsPath);
      try {
        admin.initialize(initOpts);
      } catch (Exception e) {
        System.out.println("Initialization failed due to the following exception:");
        e.printStackTrace();
        System.exit(-1);
      }
      System.out.println("Initialization is complete.");
    }
  }
}
