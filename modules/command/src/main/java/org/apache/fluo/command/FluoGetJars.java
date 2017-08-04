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
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FluoGetJars {

  private static final Logger log = LoggerFactory.getLogger(FluoGetJars.class);

  public static void main(String[] args) {
    if (args.length != 3) {
      System.err
          .println("Usage: FluoGetJars <connectionPropsPath> <applicationName> <downloadPath>");
      System.exit(-1);
    }
    String connectionPropsPath = args[0];
    String applicationName = args[1];
    String downloadPath = args[2];
    Objects.requireNonNull(connectionPropsPath);
    Objects.requireNonNull(applicationName);
    File connectionPropsFile = new File(connectionPropsPath);
    Preconditions.checkArgument(connectionPropsFile.exists(), connectionPropsPath
        + " does not exist");

    FluoConfiguration config = new FluoConfiguration(connectionPropsFile);
    config.setApplicationName(applicationName);
    CommandUtil.verifyAppInitialized(config);

    config = FluoAdminImpl.mergeZookeeperConfig(config);

    if (config.getObserverJarsUrl().isEmpty()) {
      log.info("No observer jars found for the '{}' Fluo application!", applicationName);
      return;
    }

    try {
      if (config.getObserverJarsUrl().startsWith("hdfs://")) {
        FileSystem fs = FileSystem.get(new URI(config.getDfsRoot()), new Configuration());
        File downloadPathFile = new File(downloadPath);
        if (downloadPathFile.exists()) {
          FileUtils.deleteDirectory(downloadPathFile);
        }
        fs.copyToLocalFile(new Path(config.getObserverJarsUrl()), new Path(downloadPath));
      } else {
        log.error("Unsupported url prefix for {}={}", FluoConfiguration.OBSERVER_JARS_URL_PROP,
            config.getObserverJarsUrl());
      }
    } catch (Exception e) {
      log.error("", e);
    }
  }
}
