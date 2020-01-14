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
import java.util.Optional;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;

@Parameters(commandNames = "init",
    commandDescription = "Initializes Fluo application for <app> using <appProps>")
public class FluoInit extends AppCommand {

  @Parameter(names = "-p", required = true, description = "Path to application properties file")
  private String appPropsPath;

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

  @Parameter(names = "--retrieveProperty",
      description = "Gets specified property without initializing")
  private String retrieveProperty;

  String getAppPropsPath() {
    return appPropsPath;
  }

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

  String getRetrieveProperty() {
    return retrieveProperty;
  }

  private static boolean readYes() {
    String input;
    while (true) {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
      try {
        input = Optional.ofNullable(bufferedReader.readLine()).orElse("").trim();
      } catch (IOException e) {
        throw new FluoCommandException(e);
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

  @Override
  public void execute() throws FluoCommandException {
    File applicationPropsFile = new File(getAppPropsPath());
    Preconditions.checkArgument(applicationPropsFile.exists(),
        getAppPropsPath() + " does not exist");

    FluoConfiguration config = getConfig();
    config.load(applicationPropsFile);

    String propKey = getRetrieveProperty();
    if (propKey != null && !propKey.isEmpty()) {
      if (config.containsKey(propKey)) {
        System.out.println(config.getString(propKey));
      }
      return;
    }

    if (!config.hasRequiredAdminProps()) {
      throw new FluoCommandException(
          "Error - Required properties are not set in " + getAppPropsPath());
    }
    try {
      config.validate();
    } catch (Exception e) {
      throw new FluoCommandException("Error - Invalid configuration due to " + e.getMessage(), e);
    }

    try (FluoAdminImpl admin = new FluoAdminImpl(config)) {

      FluoAdmin.InitializationOptions initOpts = new FluoAdmin.InitializationOptions();

      if (getUpdate()) {
        System.out.println("Updating configuration for the Fluo '" + config.getApplicationName()
            + "' application in Zookeeper using " + getAppPropsPath());
        admin.updateSharedConfig();
        System.out.println("Update is complete.");
        return;
      }

      if (getForce()) {
        initOpts.setClearZookeeper(true).setClearTable(true);
      } else {
        if (getClearZookeeper()) {
          initOpts.setClearZookeeper(true);
        } else if (admin.zookeeperInitialized()) {
          System.out.print("A Fluo '" + config.getApplicationName()
              + "' application is already initialized in Zookeeper at " + config.getAppZookeepers()
              + " - Would you like to clear and reinitialize Zookeeper"
              + " for this application (y/n)? ");
          if (readYes()) {
            initOpts.setClearZookeeper(true);
          } else {
            throw new FluoCommandException("Aborted initialization.");
          }
        }

        if (getClearTable()) {
          initOpts.setClearTable(true);
        } else if (admin.accumuloTableExists()) {
          System.out.print("The Accumulo table '" + config.getAccumuloTable()
              + "' already exists - Would you like to drop and recreate this table (y/n)? ");
          if (readYes()) {
            initOpts.setClearTable(true);
          } else {
            throw new FluoCommandException("Aborted initialization.");
          }
        }
      }

      System.out.println("Initializing Fluo '" + config.getApplicationName()
          + "' application using " + getAppPropsPath());

      admin.initialize(initOpts);
      System.out.println("Initialization is complete.");
    } catch (FluoAdmin.AlreadyInitializedException | FluoAdmin.TableExistsException e) {
      throw new FluoCommandException(e.getMessage(), e);
    }
  }
}
