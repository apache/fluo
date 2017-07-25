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

import java.io.File;
import java.net.URI;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FluoGetProp {

  public static void main(String[] args) {
    if (args.length != 3) {
      System.err
          .println("Usage: FluoGetProps <connectionPropsPath> <applicationPropsPath> <property>");
      System.exit(-1);
    }
    final String connectionPropsPath = args[0];
    final String applicationPropsPath = args[1];
    final String prop = args[2];
    Objects.requireNonNull(connectionPropsPath);
    Objects.requireNonNull(applicationPropsPath);
    File connectionPropsFile = new File(connectionPropsPath);
    File applicationPropsFile = new File(applicationPropsPath);
    Preconditions.checkArgument(connectionPropsFile.exists(), connectionPropsPath
        + " does not exist");
    Preconditions.checkArgument(applicationPropsFile.exists(), applicationPropsPath
        + " does not exist");

    FluoConfiguration config = new FluoConfiguration(connectionPropsFile);
    config.load(applicationPropsFile);

    if (config.containsKey(prop)) {
      System.out.println(config.getString(prop));
    }
  }
}
