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

import java.io.FileReader;
import java.util.Properties;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import accismus.api.Admin;
import accismus.api.Admin.AlreadyInitializedException;
import accismus.impl.Configuration;

/**
 * 
 */
public class InitializeTool extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new InitializeTool(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage : " + InitializeTool.class.getSimpleName() + " <props file>  <init file>");
      System.exit(-1);
    }
    
    Properties props = new Properties(Configuration.getDefaultProperties());
    props.load(new FileReader(args[0]));
    props.load(new FileReader(args[1]));
    
    try {
      Admin.initialize(props);
    } catch (AlreadyInitializedException aie) {
      Admin.updateWorkerConfig(props);
    }

    return 0;
  }
}
