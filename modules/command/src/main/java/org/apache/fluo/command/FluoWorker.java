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
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FluoWorker {

  private static final Logger log = LoggerFactory.getLogger(FluoWorker.class);

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: FluoWorker <connectionPropsPath> <applicationName>");
      System.exit(-1);
    }
    String connectionPropsPath = args[0];
    String applicationName = args[1];
    Objects.requireNonNull(connectionPropsPath);
    Objects.requireNonNull(applicationName);
    File connectionPropsFile = new File(connectionPropsPath);
    Preconditions.checkArgument(connectionPropsFile.exists(), connectionPropsPath
        + " does not exist");

    try {
      FluoConfiguration config = new FluoConfiguration(connectionPropsFile);
      config.setApplicationName(applicationName);
      CommandUtil.verifyAppInitialized(config);
      org.apache.fluo.api.service.FluoWorker worker = FluoFactory.newWorker(config);
      worker.start();
      while (true) {
        UtilWaitThread.sleep(10000);
      }
    } catch (Exception e) {
      log.error("Exception running FluoWorker: ", e);
    }
  }
}
