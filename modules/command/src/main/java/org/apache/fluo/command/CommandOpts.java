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

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.fluo.api.config.FluoConfiguration;

public class CommandOpts {

  @Parameter(names = {"-D"}, description = "Sets configuration property. Expected format: <name>=<value>")
  private List<String> properties = new ArrayList<>();

  @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
  boolean help;

  List<String> getProperties() {
    return properties;
  }

  void overrideConfig(FluoConfiguration config) {
    for (String prop : getProperties()) {
      String[] propArgs = prop.split("=");
      if (propArgs.length == 2) {
        String key = propArgs[0].trim();
        String value = propArgs[1].trim();
        if (!key.isEmpty() && !value.isEmpty()) {
          config.setProperty(key, value);
        }
      }
    }
  }

  public static CommandOpts parse(String programName, String[] args) {
    CommandOpts commandOpts = new CommandOpts();
    JCommander jcommand = new JCommander(commandOpts);
    jcommand.setProgramName(programName);
    try {
      jcommand.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      jcommand.usage();
      System.exit(-1);
    }

    if (commandOpts.help) {
      jcommand.usage();
      System.exit(1);
    }
    return commandOpts;
  }
}