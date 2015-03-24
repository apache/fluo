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
package io.fluo.cluster.util;

import java.io.IOException;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import io.fluo.api.config.FluoConfiguration;
import org.apache.commons.configuration.ConfigurationConverter;

public class MainOptions {

  public final static String STDOUT = "STDOUT";
  
  @Parameter(names = "-config-dir", description = "Location of Fluo configuration directory")
  private String configDir;

  @Parameter(names = "-log-output", description = "Location to output logging.  Set to directory or STDOUT (which is default)")
  private String logOutput = STDOUT;
    
  @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
  public boolean help;
  
  public String getConfigDir() {
    return configDir;
  }
  
  public String getFluoProps() {
    return configDir + "/fluo.properties";
  }
 
  public String getLogOutput() {
    return logOutput;
  }
  
  public void validateConfig() throws IOException {
    if (getConfigDir() == null) { 
      System.err.println("Please set -config-dir option to directory containing fluo.properties file like below: ");
      System.err.println();
      Properties defaults = ConfigurationConverter.getProperties(FluoConfiguration.getDefaultConfiguration());
      defaults.store(System.err, "Fluo properties");
      System.exit(-1);
    }
  }
}
