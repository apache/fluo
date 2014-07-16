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
package io.fluo.tools;

import java.util.Properties;

import io.fluo.api.Admin;
import io.fluo.api.Admin.AlreadyInitializedException;
import io.fluo.cluster.util.Logging;
import io.fluo.core.util.PropertyUtil;
import io.fluo.yarn.RunnableOptions;

import com.beust.jcommander.JCommander;

/** Initializes Fluo using properties in configuration files
 */
public class InitializeTool {

  public static void main(String[] args) throws Exception {

    RunnableOptions options = new RunnableOptions();
    JCommander jcommand = new JCommander(options, args);

    if (options.help) {
      jcommand.usage();
      System.exit(-1);
    }
    options.validateConfig();

    Logging.init("init", options.getConfigDir(), options.getLogOutput());

    String[] configs = { options.getInitConfig(), options.getFluoConfig() };
    Properties props = PropertyUtil.loadProps(configs);

    try {
      //TODO maybe use commons Configuration instrea of Properties in API
      Admin.initialize(props);
    } catch (AlreadyInitializedException aie) {
      Admin.updateWorkerConfig(props);
    }
  }
}
