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

package org.apache.fluo.cluster.runner;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.config.FluoConfiguration;

/**
 * Runs applications in MiniFluo
 */
public class MiniAppRunner extends AppRunner {

  public MiniAppRunner() {
    super("mini-fluo");
  }

  public void cleanup(FluoConfiguration appConfig) {
    File dataDir = new File(appConfig.getMiniDataDir());
    if (dataDir.exists() && appConfig.getMiniStartAccumulo()) {
      try {
        FileUtils.deleteDirectory(dataDir);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }

}
