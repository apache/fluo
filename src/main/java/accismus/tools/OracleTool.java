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

import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import accismus.impl.Configuration;
import accismus.impl.Logging;
import accismus.impl.OracleServer;

/**
 * 
 */
public class OracleTool extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new OracleTool(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Please supply a propeties files with the following defined : ");
      System.err.println();
      Configuration.getDefaultProperties().store(System.err, "Accismus properties");
      System.exit(-1);
    }
    
    Logging.init("oracle");
    
    Configuration config = new Configuration(InitializeTool.loadProps(args[0]));
    
    OracleServer server = new OracleServer(config);
    server.start();
    
    while (true) {
      UtilWaitThread.sleep(10000);
    }
  }
}

