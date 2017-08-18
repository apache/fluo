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

package org.apache.fluo.cluster.runner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;

/**
 * For running Fluo app on cluster
 */
@Deprecated
public abstract class ClusterAppRunner extends AppRunner {

  public ClusterAppRunner(String scriptName) {
    super(scriptName);
  }

  public class InitOptions {

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
    public boolean help;

    public boolean getForce() {
      return force;
    }

    public boolean getClearTable() {
      return clearTable;
    }

    public boolean getClearZookeeper() {
      return clearZookeeper;
    }

    public boolean getUpdate() {
      return update;
    }
  }

  public static boolean readYes() {
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

  public void init(FluoConfiguration config, String propsPath, String[] args) {
    InitOptions commandOpts = new InitOptions();
    JCommander jcommand = new JCommander(commandOpts);
    jcommand.setProgramName("fluo init");
    try {
      jcommand.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      jcommand.usage();
      System.exit(-1);
    }

    if (commandOpts.help) {
      jcommand.usage();
      System.exit(0);
    }

    if (!config.hasRequiredAdminProps()) {
      System.err.println("Error - Required properties are not set in " + propsPath);
      System.exit(-1);
    }
    try {
      config.validate();
    } catch (IllegalArgumentException e) {
      System.err.println("Error - Invalid fluo.properties (" + propsPath + ") due to "
          + e.getMessage());
      System.exit(-1);
    } catch (Exception e) {
      System.err.println("Error - Invalid fluo.properties (" + propsPath + ") due to "
          + e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }

    try (FluoAdminImpl admin = new FluoAdminImpl(config)) {

      if (admin.applicationRunning()) {
        System.err.println("Error - The Fluo '" + config.getApplicationName() + "' application"
            + " is already running and must be stopped before running 'fluo init'. "
            + " Aborted initialization.");
        System.exit(-1);
      }

      FluoAdmin.InitializationOptions initOpts = new FluoAdmin.InitializationOptions();

      if (commandOpts.getUpdate()) {
        System.out.println("Updating configuration for the Fluo '" + config.getApplicationName()
            + "' application in Zookeeper using " + propsPath);
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
          + "' application using " + propsPath);
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
