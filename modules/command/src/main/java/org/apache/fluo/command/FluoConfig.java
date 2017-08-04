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
import java.net.URI;
import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FluoConfig {

  private static final Logger log = LoggerFactory.getLogger(FluoConfig.class);

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: FluoConfig <connectionPropsPath> <applicationName>");
      System.exit(-1);
    }
    String connectionPropsPath = args[0];
    String applicationName = args[1];
    Objects.requireNonNull(connectionPropsPath);
    Objects.requireNonNull(applicationName);
    File connectionPropsFile = new File(connectionPropsPath);
    Preconditions.checkArgument(connectionPropsFile.exists(), connectionPropsPath
        + " does not exist");

    FluoConfiguration config = new FluoConfiguration(connectionPropsFile);
    config.setApplicationName(applicationName);
    CommandUtil.verifyAppInitialized(config);

    try (FluoAdmin admin = FluoFactory.newAdmin(config)) {
      for (Map.Entry<String, String> entry : admin.getApplicationConfig().toMap().entrySet()) {
        System.out.println(entry.getKey() + " = " + entry.getValue());
      }
    }
  }
}
