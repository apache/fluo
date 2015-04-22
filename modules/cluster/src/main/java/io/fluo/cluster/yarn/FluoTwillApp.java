/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.cluster.yarn;

import java.io.File;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.cluster.main.FluoOracleMain;
import io.fluo.cluster.main.FluoWorkerMain;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.ResourceSpecification.SizeUnit;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.TwillSpecification.Builder.LocalFileAdder;
import org.apache.twill.api.TwillSpecification.Builder.MoreFile;
import org.apache.twill.api.TwillSpecification.Builder.MoreRunnable;
import org.apache.twill.api.TwillSpecification.Builder.RunnableSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents Fluo oracle application in Twill
 */
public class FluoTwillApp implements TwillApplication {

  private static final Logger log = LoggerFactory.getLogger(FluoTwillApp.class);

  private final FluoConfiguration config;
  private final String fluoConf;

  public FluoTwillApp(FluoConfiguration config, String flouConf) {
    this.config = config;
    this.fluoConf = flouConf;
  }

  private MoreFile addConfigFiles(LocalFileAdder fileAdder) {
    File confDir = new File(fluoConf);
    MoreFile moreFile = null;
    for (File f : confDir.listFiles()) {
      if (f.isFile()) {
        log.trace("Adding config file - " + f.getAbsolutePath());
        if (moreFile == null) {
          moreFile = fileAdder.add(String.format("./conf/%s", f.getName()), f);
        } else {
          moreFile = moreFile.add(String.format("./conf/%s", f.getName()), f);
        }
      }
    }
    return moreFile;
  }

  @Override
  public TwillSpecification configure() {

    log.info("Configuring Fluo '{}' application with {} Oracle instances and {} Worker instances "
        + "with following properties:", config.getApplicationName(), config.getOracleInstances(),
        config.getWorkerInstances());

    log.info("{} = {}", FluoConfiguration.ORACLE_MAX_MEMORY_MB_PROP, config.getOracleMaxMemory());
    log.info("{} = {}", FluoConfiguration.WORKER_MAX_MEMORY_MB_PROP, config.getWorkerMaxMemory());
    log.info("{} = {}", FluoConfiguration.ORACLE_NUM_CORES_PROP, config.getOracleNumCores());
    log.info("{} = {}", FluoConfiguration.WORKER_NUM_CORES_PROP, config.getWorkerNumCores());

    // Start building Fluo Twill application
    MoreRunnable moreRunnable =
        TwillSpecification.Builder.with().setName(config.getApplicationName()).withRunnable();

    // Configure Oracle
    ResourceSpecification oracleResources =
        ResourceSpecification.Builder.with().setVirtualCores(config.getOracleNumCores())
            .setMemory(config.getOracleMaxMemory(), SizeUnit.MEGA)
            .setInstances(config.getOracleInstances()).build();

    LocalFileAdder fileAdder =
        moreRunnable.add(FluoOracleMain.ORACLE_NAME, new FluoOracleMain(), oracleResources)
            .withLocalFiles();
    RunnableSetter runnableSetter = addConfigFiles(fileAdder).apply();

    // Configure Worker
    ResourceSpecification workerResources =
        ResourceSpecification.Builder.with().setVirtualCores(config.getWorkerNumCores())
            .setMemory(config.getWorkerMaxMemory(), SizeUnit.MEGA)
            .setInstances(config.getWorkerInstances()).build();

    fileAdder =
        runnableSetter.add(FluoWorkerMain.WORKER_NAME, new FluoWorkerMain(), workerResources)
            .withLocalFiles();
    runnableSetter = addConfigFiles(fileAdder).apply();

    // Set runnable order, build and return TwillSpecification
    return runnableSetter.withOrder().begin(FluoOracleMain.ORACLE_NAME)
        .nextWhenStarted(FluoWorkerMain.WORKER_NAME).build();
  }
}
