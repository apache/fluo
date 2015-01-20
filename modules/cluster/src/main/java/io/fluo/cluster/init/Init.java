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
package io.fluo.cluster.init;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import io.fluo.api.client.FluoAdmin;
import io.fluo.api.client.FluoAdmin.InitOpts;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.core.client.FluoAdminImpl;
import org.slf4j.LoggerFactory;

/** 
 * Initializes Fluo using properties in configuration files
 */
public class Init {
  
  public static boolean readYes() throws IOException {
    String input = "unk";
    while (true) {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in));
      input = bufferedReader.readLine().trim();
      if (input.equalsIgnoreCase("y")) {
        return true;
      } else if (input.equalsIgnoreCase("n")) {
        return false;
      } else {
        System.out.print("Unexpected input '" + input + "'. Enter y/n or ctrl-c to abort: ");
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    
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
    
    Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.ERROR);

    FluoConfiguration config = new FluoConfiguration(new File(commandOpts.getFluoProps()));
    if (!config.hasRequiredAdminProps()) {
      System.err.println("Error - Required properties are not set in " + commandOpts.getFluoProps());
      System.exit(-1);
    }
    try {
      config.validate();
    } catch (IllegalArgumentException e) {
      System.err.println("Error - Invalid fluo.properties ("+commandOpts.getFluoProps()+") due to "+ e.getMessage());
      System.exit(-1);
    } catch (Exception e) {
      System.err.println("Error - Invalid fluo.properties ("+commandOpts.getFluoProps()+") due to "+ e.getMessage());
      e.printStackTrace();
      System.exit(-1);
    }
        
    if (FluoAdminImpl.oracleExists(config)) {
      System.err.println("Error - A Fluo instance is running and must be stopped before running 'fluo init'.  Aborted initialization.");
      System.exit(-1);
    }

    InitOpts initOpts = new InitOpts();
    
    if (commandOpts.getUpdate()) {
      System.out.println("Updating Fluo configuration in Zookeeper using " + commandOpts.getFluoProps());
      FluoAdmin admin = FluoFactory.newAdmin(config);
      admin.updateSharedConfig();
      System.out.println("Update is complete.");
      System.exit(0);
    } 
    
    if (commandOpts.getForce()) {
      initOpts.setClearZookeeper(true).setClearTable(true);
    } else {
      if (commandOpts.getClearZookeeper()) {
        initOpts.setClearZookeeper(true);
      } else if (FluoAdminImpl.zookeeperInitialized(config)) {
        System.out.print("Fluo is already initialized in Zookeeper at " + config.getZookeepers() + " - Would you like to clear and reinitialize Zookeeper (y/n)? ");
        if (readYes()) {
          initOpts.setClearZookeeper(true);
        } else {
          System.out.println("Aborted initialization.");
          System.exit(-1);
        }
      }

      if (commandOpts.getClearTable()) {
        initOpts.setClearTable(true);
      } else if (FluoAdminImpl.accumuloTableExists(config)) {
        System.out.print("The Accumulo table '" + config.getAccumuloTable() + "' already exists - Would you like to drop and recreate this table (y/n)? ");
        if (readYes()) {
          initOpts.setClearTable(true);
        } else {
          System.out.println("Aborted initialization.");
          System.exit(-1);
        }
      }
    }

    System.out.println("Initializing Fluo instance using " + commandOpts.getFluoProps());
    FluoAdmin admin = FluoFactory.newAdmin(config);
    admin.initialize(initOpts);
    System.out.println("Initialization is complete.");
  }
}
