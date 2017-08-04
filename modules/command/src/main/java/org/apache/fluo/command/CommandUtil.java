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

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;

public class CommandUtil {

  public static void verifyAppInitialized(FluoConfiguration config) {
    if (!FluoAdminImpl.isInitialized(config)) {
      System.out.println("A Fluo '" + config.getApplicationName() + "' application has not "
          + "been initialized yet in Zookeeper at " + config.getAppZookeepers());
      System.exit(-1);
    }
  }

  public static void verifyAppRunning(FluoConfiguration config) {
    verifyAppInitialized(config);
    if (!FluoAdminImpl.oracleExists(config)) {
      System.out.println("A Fluo '" + config.getApplicationName() + "' application is initialized "
          + "but is not running!");
      System.exit(-1);
    }
  }
}
