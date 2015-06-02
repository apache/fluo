/*
 * Copyright 2015 Fluo authors (see AUTHORS)
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

package io.fluo.cluster.util;

import java.io.File;

import io.fluo.api.config.FluoConfiguration;

public class FluoPath {

  private String fluoHomeDir;
  private String appName;

  public FluoPath(String fluoHomeDir, String appName) {
    this.fluoHomeDir = fluoHomeDir;
    this.appName = appName;
  }

  public String getConfDir() {
    return verifyInstallPath(fluoHomeDir + "/conf");
  }

  public String getLibDir() {
    return verifyInstallPath(fluoHomeDir + "/lib");
  }

  public String getAppsDir() {
    return verifyInstallPath(fluoHomeDir + "/apps");
  }

  public String getAppPropsPath() {
    return verifyAppPath(getAppConfDir() + "/fluo.properties");
  }

  public String getAppConfDir() {
    return verifyAppPath(String.format("%s/%s/conf", getAppsDir(), appName));
  }

  public String getAppLibDir() {
    return verifyAppPath(String.format("%s/%s/lib", getAppsDir(), appName));
  }

  public FluoConfiguration getAppConfiguration() {
    FluoConfiguration config = new FluoConfiguration(new File(getAppPropsPath()));
    if (!config.getFluoApplicationName().equals(appName)) {
      throw new IllegalStateException("Application name in config '"
          + config.getFluoApplicationName() + "' does not match given appName: " + appName);
    }
    return config;
  }

  private String verifyInstallPath(String path) {
    File f = new File(path);
    if (!f.exists()) {
      throw new IllegalStateException("Path does not exist in Fluo install: " + path);
    }
    return path;
  }

  private String verifyAppPath(String path) {
    File f = new File(path);
    if (!f.exists()) {
      throw new IllegalStateException("Path does not exists for Fluo '" + appName
          + "' application: " + path);
    }
    return path;
  }
}
