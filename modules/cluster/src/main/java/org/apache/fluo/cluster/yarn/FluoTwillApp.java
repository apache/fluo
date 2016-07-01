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

package org.apache.fluo.cluster.yarn;

import java.io.File;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.cluster.runnable.OracleRunnable;
import org.apache.fluo.cluster.runnable.WorkerRunnable;
import org.apache.fluo.cluster.runner.YarnAppRunner;
import org.apache.fluo.cluster.util.FluoYarnConfig;
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
 * Represents Fluo application in Twill
 */
public class FluoTwillApp implements TwillApplication {

  private static final Logger log = LoggerFactory.getLogger(FluoTwillApp.class);

  private final FluoConfiguration config;
  private final String fluoConf;

  public FluoTwillApp(FluoConfiguration config, String fluoConf) {
    this.config = config;
    this.fluoConf = fluoConf;
  }

  private MoreFile addConfigFiles(LocalFileAdder fileAdder) {
    File confDir = new File(fluoConf);
    MoreFile moreFile = null;
    File[] confFiles = confDir.listFiles();
    if (confFiles != null) {
      for (File f : confFiles) {
        if (f.isFile()) {
          log.trace("Adding config file - " + f.getAbsolutePath());
          if (moreFile == null) {
            moreFile = fileAdder.add(String.format("./conf/%s", f.getName()), f);
          } else {
            moreFile = moreFile.add(String.format("./conf/%s", f.getName()), f);
          }
        }
      }
    }
    return moreFile;
  }

  @Override
  public TwillSpecification configure() {

    final int oracleInstances = FluoYarnConfig.getOracleInstances(config);
    final int oracleMaxMemory = FluoYarnConfig.getOracleMaxMemory(config);
    final int oracleNumCores = FluoYarnConfig.getOracleNumCores(config);
    final int workerInstances = FluoYarnConfig.getWorkerInstances(config);
    final int workerMaxMemory = FluoYarnConfig.getWorkerMaxMemory(config);
    final int workerNumCores = FluoYarnConfig.getWorkerNumCores(config);

    log.info("Configuring Fluo '{}' application with {} Oracle instances and {} Worker instances "
        + "with following properties:", config.getApplicationName(), oracleInstances,
        workerInstances);

    log.info("{} = {}", FluoYarnConfig.ORACLE_MAX_MEMORY_MB_PROP, oracleMaxMemory);
    log.info("{} = {}", FluoYarnConfig.WORKER_MAX_MEMORY_MB_PROP, workerMaxMemory);
    log.info("{} = {}", FluoYarnConfig.ORACLE_NUM_CORES_PROP, oracleNumCores);
    log.info("{} = {}", FluoYarnConfig.WORKER_NUM_CORES_PROP, workerNumCores);

    // Start building Fluo Twill application
    MoreRunnable moreRunnable =
        TwillSpecification.Builder.with()
            .setName(YarnAppRunner.getYarnApplicationName(config.getApplicationName()))
            .withRunnable();

    // Configure Oracle(s)
    ResourceSpecification oracleResources =
        ResourceSpecification.Builder.with().setVirtualCores(oracleNumCores)
            .setMemory(oracleMaxMemory, SizeUnit.MEGA).setInstances(oracleInstances).build();

    LocalFileAdder fileAdder =
        moreRunnable.add(OracleRunnable.ORACLE_NAME, new OracleRunnable(), oracleResources)
            .withLocalFiles();
    RunnableSetter runnableSetter = addConfigFiles(fileAdder).apply();

    // Configure Worker(s)
    ResourceSpecification workerResources =
        ResourceSpecification.Builder.with().setVirtualCores(workerNumCores)
            .setMemory(workerMaxMemory, SizeUnit.MEGA).setInstances(workerInstances).build();

    fileAdder =
        runnableSetter.add(WorkerRunnable.WORKER_NAME, new WorkerRunnable(), workerResources)
            .withLocalFiles();
    runnableSetter = addConfigFiles(fileAdder).apply();

    // Set runnable order, build and return TwillSpecification
    return runnableSetter.withOrder().begin(OracleRunnable.ORACLE_NAME)
        .nextWhenStarted(WorkerRunnable.WORKER_NAME).build();
  }
}
