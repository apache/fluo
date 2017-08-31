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

import com.beust.jcommander.Parameter;

class CommonOpts extends ConfigOpts {

  @Parameter(names = "-a", required = true, description = "Fluo application name")
  private String applicationName;

  String getApplicationName() {
    return applicationName;
  }

  public static CommonOpts parse(String programName, String[] args) {
    CommonOpts opts = new CommonOpts();
    parse(programName, opts, args);
    return opts;
  }
}
