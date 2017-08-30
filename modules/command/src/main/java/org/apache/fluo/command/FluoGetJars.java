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

import com.beust.jcommander.Parameter;
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

  public static class GetJarsOpts extends CommonOpts {

    @Parameter(names = "-d", required = true, description = "Download directory path")
    private String downloadPath;

    String getDownloadPath() {
      return downloadPath;
    }

    public static GetJarsOpts parse(String[] args) {
      GetJarsOpts opts = new GetJarsOpts();
      parse("fluo get-jars", opts, args);
      return opts;
    }
  }

  public static void main(String[] args) {

    GetJarsOpts opts = GetJarsOpts.parse(args);

    FluoConfiguration config = CommandUtil.resolveFluoConfig();
    config.setApplicationName(opts.getApplicationName());
    opts.overrideFluoConfig(config);

    CommandUtil.verifyAppInitialized(config);
    config = FluoAdminImpl.mergeZookeeperConfig(config);
    if (config.getObserverJarsUrl().isEmpty()) {
      log.info("No observer jars found for the '{}' Fluo application!", opts.getApplicationName());
      return;
    }

    try {
      if (config.getObserverJarsUrl().startsWith("hdfs://")) {
        FileSystem fs = FileSystem.get(new URI(config.getDfsRoot()), new Configuration());
        File downloadPathFile = new File(opts.getDownloadPath());
        if (downloadPathFile.exists()) {
          FileUtils.deleteDirectory(downloadPathFile);
        }
        fs.copyToLocalFile(new Path(config.getObserverJarsUrl()), new Path(opts.getDownloadPath()));
      } else {
        log.error("Unsupported url prefix for {}={}", FluoConfiguration.OBSERVER_JARS_URL_PROP,
            config.getObserverJarsUrl());
      }
    } catch (Exception e) {
      log.error("", e);
    }
  }
}
