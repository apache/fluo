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
import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
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

  public static class GetJarsOpts {

    @Parameter(names = "-a", required = true, description = "Fluo application name")
    private String applicationName;

    @Parameter(names = "-d", required = true, description = "Download directory path")
    private String downloadPath;

    @Parameter(names = "-D", description = "Sets configuration property. Expected format: <name>=<value>")
    private List<String> properties = new ArrayList<>();

    @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
    boolean help;

    String getApplicationName() {
      return applicationName;
    }

    public String getDownloadPath() {
      return downloadPath;
    }

    List<String> getProperties() {
      return properties;
    }
  }

  public static void main(String[] args) {

    GetJarsOpts options = new GetJarsOpts();
    JCommander jcommand = new JCommander(options);
    jcommand.setProgramName("fluo get-jars");
    try {
      jcommand.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      jcommand.usage();
      System.exit(-1);
    }

    if (options.help) {
      jcommand.usage();
      System.exit(0);
    }

    FluoConfiguration config = CommandUtil.resolveFluoConfig();
    config.setApplicationName(options.getApplicationName());
    CommandUtil.overrideFluoConfig(config, options.getProperties());

    CommandUtil.verifyAppInitialized(config);
    config = FluoAdminImpl.mergeZookeeperConfig(config);
    if (config.getObserverJarsUrl().isEmpty()) {
      log.info("No observer jars found for the '{}' Fluo application!", options.getApplicationName());
      return;
    }

    try {
      if (config.getObserverJarsUrl().startsWith("hdfs://")) {
        FileSystem fs = FileSystem.get(new URI(config.getDfsRoot()), new Configuration());
        File downloadPathFile = new File(options.getDownloadPath());
        if (downloadPathFile.exists()) {
          FileUtils.deleteDirectory(downloadPathFile);
        }
        fs.copyToLocalFile(new Path(config.getObserverJarsUrl()), new Path(options.getDownloadPath()));
      } else {
        log.error("Unsupported url prefix for {}={}", FluoConfiguration.OBSERVER_JARS_URL_PROP,
            config.getObserverJarsUrl());
      }
    } catch (Exception e) {
      log.error("", e);
    }
  }
}
