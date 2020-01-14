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
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Parameters(commandNames = "get-jars",
    commandDescription = "Copies <app> jars from DFS to local <dir>")
public class FluoGetJars extends AppCommand {

  private static final Logger log = LoggerFactory.getLogger(FluoGetJars.class);

  @Parameter(names = "-d", required = true, description = "Download directory path")
  private String downloadPath;

  String getDownloadPath() {
    return downloadPath;
  }

  @Override
  public void execute() throws FluoCommandException {
    FluoConfiguration config = getConfig();

    CommandUtil.verifyAppInitialized(config);
    config = FluoAdminImpl.mergeZookeeperConfig(config);
    if (config.getObserverJarsUrl().isEmpty()) {
      log.info("No observer jars found for the '{}' Fluo application!", getApplicationName());
      return;
    }

    if (config.getObserverJarsUrl().startsWith("hdfs://")) {
      try (FileSystem fs = FileSystem.get(new URI(config.getDfsRoot()), new Configuration())) {
        File downloadPathFile = new File(getDownloadPath());
        if (downloadPathFile.exists()) {
          FileUtils.deleteDirectory(downloadPathFile);
        }
        fs.copyToLocalFile(new Path(config.getObserverJarsUrl()), new Path(getDownloadPath()));
      } catch (URISyntaxException e) {
        throw new FluoCommandException(
            String.format("Error parsing DFS ROOT URI: %s", e.getMessage()), e);
      } catch (IOException e) {
        throw new FluoCommandException(e);
      }
    } else {
      throw new FluoCommandException(String.format("Unsupported url prefix for %s=%s",
          FluoConfiguration.OBSERVER_JARS_URL_PROP, config.getObserverJarsUrl()));
    }
  }
}
