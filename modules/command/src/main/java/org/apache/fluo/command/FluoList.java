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
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.fluo.core.util.CuratorUtil;

public class FluoList {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: FluoList <connectionPropsPath>");
      System.exit(-1);
    }
    String connectionPropsPath = args[0];
    Objects.requireNonNull(connectionPropsPath);
    File connectionPropsFile = new File(connectionPropsPath);
    Preconditions.checkArgument(connectionPropsFile.exists(), connectionPropsPath
        + " does not exist");

    FluoConfiguration config = new FluoConfiguration(connectionPropsFile);

    try (CuratorFramework curator = CuratorUtil.newFluoCurator(config)) {
      curator.start();

      if (curator.checkExists().forPath("/") == null) {
        System.out.println("Fluo instance (" + config.getInstanceZookeepers() + ") has not been "
            + "created yet in Zookeeper.  It will be created when the first Fluo application is "
            + "initialized for this instance.");
        return;
      }
      List<String> children = curator.getChildren().forPath("/");
      if (children.isEmpty()) {
        System.out.println("Fluo instance (" + config.getInstanceZookeepers() + ") does not "
            + "contain any Fluo applications.");
        return;
      }
      Collections.sort(children);

      System.out.println("Fluo instance (" + config.getInstanceZookeepers() + ") contains "
          + children.size() + " application(s)\n");
      System.out.println("Application     Status");
      System.out.println("-----------     ------");
      for (String path : children) {
        FluoConfiguration appConfig = new FluoConfiguration(config);
        appConfig.setApplicationName(path);
        String state = "STOPPED";
        if (FluoAdminImpl.oracleExists(appConfig)) {
          state = "RUNNING";
        }
        System.out.format("%-15s %-11s\n", path, state);
      }
    }
  }
}
