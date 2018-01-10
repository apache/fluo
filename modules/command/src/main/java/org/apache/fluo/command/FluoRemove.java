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

import java.io.File;

import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;

public class FluoRemove {

  public static class RemoveOptions extends CommonOpts {

    @Parameter(names = "-p", required = true, description = "Path to application properties file")
    private String appPropsPath;

    String getAppPropsPath() {
      return appPropsPath;
    }

    public static RemoveOptions parse(String[] args) {
      RemoveOptions opts = new RemoveOptions();
      parse("fluo remove", opts, args);
      return opts;
    }
  }

  public static void main(String[] args) {
    RemoveOptions opts = RemoveOptions.parse(args);
    File applicationPropsFile = new File(opts.getAppPropsPath());
    Preconditions.checkArgument(applicationPropsFile.exists(),
        opts.getAppPropsPath() + " does not exist");

    FluoConfiguration config = CommandUtil.resolveFluoConfig();
    config.load(applicationPropsFile);
    config.setApplicationName(opts.getApplicationName());
    opts.overrideFluoConfig(config);

    if (!config.hasRequiredAdminProps()) {
      System.err.println("Error - Required properties are not set in " + opts.getAppPropsPath());
      System.exit(-1);
    }
    try {
      config.validate();
    } catch (Exception e) {
      System.err.println("Error - Invalid configuration due to " + e.getMessage());
      System.exit(-1);
    }

    try (FluoAdminImpl admin = new FluoAdminImpl(config)) {

      if (admin.applicationRunning()) {
        System.err.println("Error - The Fluo '" + config.getApplicationName() + "' application"
            + " is already running and must be stopped before running 'fluo remove'. "
            + " Aborted remove.");
        System.exit(-1);
      }

      FluoAdmin.InitializationOptions initOpts = new FluoAdmin.InitializationOptions();
      initOpts.setClearZookeeper(true).setClearTable(true);
      initOpts.setClearTable(true);

      System.out.println("Removing Fluo '" + config.getApplicationName() + "' application using "
          + opts.getAppPropsPath());
      try {
        admin.remove(initOpts);
      } catch (Exception e) {
        System.out.println("Remove failed due to the following exception:");
        e.printStackTrace();
        System.exit(-1);
      }
      System.out.println("Remove is complete.");

    }
  }
}
