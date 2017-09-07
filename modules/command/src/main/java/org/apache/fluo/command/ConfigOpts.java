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
import java.util.Collections;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IParameterSplitter;
import org.apache.fluo.api.config.FluoConfiguration;

public class ConfigOpts extends BaseOpts {

  public static class NullSplitter implements IParameterSplitter {
    @Override
    public List<String> split(String value) {
      return Collections.singletonList(value);
    }
  }

  @Parameter(names = "-o", splitter = NullSplitter.class,
      description = "Override configuration set in properties file. Expected format: -o <key>=<value>")
  private List<String> properties = new ArrayList<>();

  List<String> getProperties() {
    return properties;
  }

  void overrideFluoConfig(FluoConfiguration config) {
    for (String prop : getProperties()) {
      String[] propArgs = prop.split("=", 2);
      if (propArgs.length == 2) {
        String key = propArgs[0].trim();
        String value = propArgs[1].trim();
        if (key.isEmpty() || value.isEmpty()) {
          throw new IllegalArgumentException("Invalid command line -D option: " + prop);
        } else {
          config.setProperty(key, value);
        }
      } else {
        throw new IllegalArgumentException("Invalid command line -D option: " + prop);
      }
    }
  }

  public static ConfigOpts parse(String programName, String[] args) {
    ConfigOpts opts = new ConfigOpts();
    parse(programName, opts, args);
    return opts;
  }
}
