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

public class ApplicationOpts {

  @Parameter(names = "-a", required = true, description = "Fluo application name")
  private String applicationName;

  @Parameter(names = "-D", description = "Sets configuration property. Expected format: <name>=<value>")
  private List<String> properties = new ArrayList<>();

  @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
  boolean help;

  String getApplicationName() {
    return applicationName;
  }

  List<String> getProperties() {
    return properties;
  }

  public static ApplicationOpts parse(String programName, String[] args) {
    ApplicationOpts commandOpts = new ApplicationOpts();
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