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

package org.apache.fluo.cluster.util;

import java.io.File;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.exceptions.FluoException;

public class FluoInstall {

  private String fluoHomeDir;

  public FluoInstall(String fluoHomeDir) {
    this.fluoHomeDir = fluoHomeDir;
  }

  public String getFluoConfDir() {
    return fluoHomeDir + "/conf";
  }

  public String getFluoPropsPath() {
    return getFluoConfDir() + "/fluo.properties";
  }

  public String getLibDir() {
    return fluoHomeDir + "/lib";
  }

  public String getAppsDir() {
    return fluoHomeDir + "/apps";
  }

  public String getAppConfDir(String appName) {
    return String.format("%s/%s/conf", getAppsDir(), appName);
  }

  public String getAppLibDir(String appName) {
    return String.format("%s/%s/lib", getAppsDir(), appName);
  }

  public String getAppPropsPath(String appName) {
    return getAppConfDir(appName) + "/fluo.properties";
  }

  public void verifyFluoInstall() {
    verifyInstallPath(fluoHomeDir);
    verifyInstallPath(getLibDir());
    verifyInstallPath(getFluoConfDir());
    verifyInstallPath(getFluoPropsPath());
  }

  public void verifyAppInstall(String appName) {
    verifyFluoInstall();
    verifyAppPath(appName, getAppsDir());
    verifyAppPath(appName, getAppConfDir(appName));
    verifyAppPath(appName, getAppPropsPath(appName));
  }

  public FluoConfiguration getAppConfiguration(String appName) {
    return getAppConfiguration(appName, true);
  }

  public FluoConfiguration getAppConfiguration(String appName, boolean debug) {
    verifyAppInstall(appName);
    String propsPath = getAppPropsPath(appName);
    FluoConfiguration config = new FluoConfiguration(new File(propsPath));
    if (!config.getApplicationName().equals(appName)) {
      throw new FluoException("Application name in config '" + config.getApplicationName()
          + "' does not match given appName: " + appName);
    }
    if (debug) {
      System.out.println("Connecting to Fluo instance (" + config.getInstanceZookeepers()
          + ") using config (" + stripFluoHomeDir(propsPath) + ")");
    }
    return config;
  }

  public String stripFluoHomeDir(String path) {
    return path.substring(fluoHomeDir.length() + 1);
  }

  public FluoConfiguration getFluoConfiguration() {
    return getFluoConfiguration(true);
  }

  public FluoConfiguration getFluoConfiguration(boolean debug) {
    verifyFluoInstall();
    String propsPath = getFluoPropsPath();
    FluoConfiguration config = new FluoConfiguration(new File(propsPath));
    if (debug) {
      System.out.println("Connecting to Fluo instance (" + config.getInstanceZookeepers()
          + ") using config (" + stripFluoHomeDir(propsPath) + ")");
    }
    return config;
  }

  public FluoConfiguration resolveFluoConfiguration(String appName) {
    return resolveFluoConfiguration(appName, true);
  }

  public FluoConfiguration resolveFluoConfiguration(String appName, boolean debug) {
    FluoConfiguration config;
    try {
      config = getAppConfiguration(appName, debug);
    } catch (FluoException e) {
      config = new FluoConfiguration(getFluoConfiguration(debug));
      config.setApplicationName(appName);
    }
    return config;
  }

  public String resolveFluoPropsPath(String appName) {
    String propsPath;
    try {
      getAppConfiguration(appName, false);
      propsPath = getAppPropsPath(appName);
    } catch (FluoException e) {
      getFluoConfiguration(false);
      propsPath = getFluoPropsPath();
    }
    return propsPath;
  }

  private void verifyInstallPath(String path) {
    if (!(new File(path).exists())) {
      throw new FluoException("Path does not exist in Fluo install: " + path);
    }
  }

  private void verifyAppPath(String appName, String path) {
    if (!(new File(path).exists())) {
      throw new FluoException("Path does not exist for Fluo '" + appName + "' application: " + path);
    }
  }
}
