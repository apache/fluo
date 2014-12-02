/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.cluster.init;

import com.beust.jcommander.Parameter;

/**
 * Fluo init command line options
 */
public class InitOptions {
  
  @Parameter(names = {"-f", "--force"}, description = "Skip all prompts and clears Zookeeper and Accumulo table.  Equivalent to setting both --clearTable --clearZookeeper")
  private boolean force;
  
  @Parameter(names = {"--clearTable"}, description = "Skips prompt and clears Accumulo table")
  private boolean clearTable;
  
  @Parameter(names = {"--clearZookeeper"}, description = "Skips prompt and clears Zookeeper")
  private boolean clearZookeeper;
  
  @Parameter(names = {"-u", "--update"}, description = "Update Fluo configuration in Zookeeper")
  private boolean update;
  
  @Parameter(names = "-config-dir", description = "Location of Fluo configuration directory", required = true, hidden = true)
  private String configDir;
  
  @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
  public boolean help;
  
  public boolean getForce() {
    return force;
  }
  
  public boolean getClearTable() {
    return clearTable;
  }
  
  public boolean getClearZookeeper() {
    return clearZookeeper;
  }
  
  public boolean getUpdate() {
    return update;
  }
  
  public String getConfigDir() {
    return configDir;
  }
  
  public String getFluoProps() {
    return configDir + "/fluo.properties";
  }
}
