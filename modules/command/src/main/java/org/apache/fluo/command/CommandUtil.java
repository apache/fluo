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

import com.google.common.base.Preconditions;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;

public class CommandUtil {

  public static final String FLUO_CONN_PROPS = "fluo.conn.props";

  public static void verifyAppInitialized(FluoConfiguration config) {
    if (!FluoAdminImpl.isInitialized(config)) {
      throw new FluoCommandException(
          "A Fluo '" + config.getApplicationName() + "' application has not "
              + "been initialized yet in Zookeeper at " + config.getAppZookeepers());
    }
  }

  public static void verifyAppRunning(FluoConfiguration config) {
    verifyAppInitialized(config);
    try (FluoAdminImpl admin = new FluoAdminImpl(config)) {
      if (!admin.applicationRunning()) {
        throw new FluoCommandException("A Fluo '" + config.getApplicationName()
            + "' application is initialized but is not running!");
      }
    }
  }

  public static FluoConfiguration resolveFluoConfig() {
    String connPropsPath = System.getProperty(FLUO_CONN_PROPS);
    if (connPropsPath == null) {
      return new FluoConfiguration();
    } else {
      File connPropsFile = new File(connPropsPath);
      Preconditions.checkArgument(connPropsFile.exists(),
          "System property 'fluo.conn.props' is set to file that doesn't exist: " + connPropsPath);
      return new FluoConfiguration(connPropsFile);
    }
  }
}
