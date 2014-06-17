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
package accismus.tools;

import java.io.File;
import java.util.Properties;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.interpol.ConfigurationInterpolator;
import org.apache.commons.lang.text.StrLookup;

import accismus.api.Admin;
import accismus.api.Admin.AlreadyInitializedException;
import accismus.cluster.util.Logging;
import accismus.impl.Configuration;
import accismus.yarn.RunnableOptions;

import com.beust.jcommander.JCommander;

/** Initializes Accismus using properties in configuration files
 */
public class InitializeTool {

  public static Properties loadProps(String ... files) throws ConfigurationException{
    ConfigurationInterpolator.registerGlobalLookup("env", new StrLookup() {
      @Override
      public String lookup(String key) {
        return System.getenv(key);
      }
    });
    
    Properties defaults = Configuration.getDefaultProperties();
    
    CompositeConfiguration compConf = new CompositeConfiguration();
    for(String file : files)
      compConf.addConfiguration(new PropertiesConfiguration(new File(file)));
    
    compConf.addConfiguration(ConfigurationConverter.getConfiguration(defaults));
    
    return ConfigurationConverter.getProperties(compConf.interpolatedConfiguration());
  }

  public static void main(String[] args) throws Exception {

    RunnableOptions options = new RunnableOptions();
    JCommander jcommand = new JCommander(options, args);

    if (options.help) {
      jcommand.usage();
      System.exit(-1);
    }
    options.validateConfig();

    Logging.init("init", options.getConfigDir(), options.getLogOutput());

    String[] configs = { options.getInitConfig(), options.getAccismusConfig() };
    Properties props = loadProps(configs);

    try {
      //TODO maybe use commons Configuration instrea of Properties in API
      Admin.initialize(props);
    } catch (AlreadyInitializedException aie) {
      Admin.updateWorkerConfig(props);
    }
  }
}
