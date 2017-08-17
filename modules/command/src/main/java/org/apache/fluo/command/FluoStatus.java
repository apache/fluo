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

public class FluoStatus {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: FluoStatus <connectionPropsPath> <applicationName>");
      System.exit(-1);
    }
    String connectionPropsPath = args[0];
    String applicationName = args[1];
    Objects.requireNonNull(connectionPropsPath);
    File connectionPropsFile = new File(connectionPropsPath);
    Preconditions.checkArgument(connectionPropsFile.exists(), connectionPropsPath
        + " does not exist");

    FluoConfiguration config = new FluoConfiguration(connectionPropsFile);
    config.setApplicationName(applicationName);

    try (FluoAdminImpl admin = new FluoAdminImpl(config)) {
      if (!admin.zookeeperInitialized()) {
        System.out.println("NOT_FOUND");
      }
      if (admin.applicationRunning()) {
        System.out.println("RUNNING");
      } else {
        System.out.println("STOPPED");
      }
    }
  }
}
