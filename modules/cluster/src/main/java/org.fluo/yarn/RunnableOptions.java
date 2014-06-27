/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fluo.yarn;

import java.io.IOException;

import org.fluo.impl.Configuration;

import com.beust.jcommander.Parameter;

public class RunnableOptions {
  
  @Parameter(names = "-config-dir", description = "Location of fluo configuration files")
  private String configDir;

  @Parameter(names = "-log-output", description = "Location to output logging.  Set to directory or STDOUT (which is default) ")
  private String logOutput = "STDOUT";
    
  @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
  public boolean help;
  
  public String getConfigDir() {
    return configDir;
  }
  
  public String getFluoConfig() {
    return configDir + "/connection.properties";
  }
  
  public String getInitConfig() {
    return configDir+"/initialization.properties";
  }
  
  public String getLogOutput() {
    return logOutput;
  }
  
  public void validateConfig() throws IOException {
    if (getConfigDir() == null) { 
      System.err.println("Please set -config-dir option to directory containing connection.properties file like below: ");
      System.err.println();
      Configuration.getDefaultProperties().store(System.err, "Fluo connection properties");
      System.exit(-1);
    }
  }
}
