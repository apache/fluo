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

package org.apache.fluo.cluster.runner;

import com.beust.jcommander.Parameter;

public class ClasspathOptions {

  @Parameter(names = {"-a", "--accumulo"}, description = "Additionally prints Accumulo client jars")
  private boolean accumulo = false;

  @Parameter(names = {"-z", "--zookeeper"},
      description = "Additionally prints Zookeeper client jars")
  private boolean zookeeper = false;

  @Parameter(names = {"-H", "--hadoop"}, description = "Additionally prints Hadoop client jars")
  private boolean hadoop = false;

  @Parameter(names = {"-l", "--lib-jars"},
      description = "Prints classpath in format suitable for Hadoop -libjars")
  private boolean libJars = false;

  @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
  public boolean help;

  public boolean getAccumulo() {
    return accumulo;
  }

  public boolean getHadoop() {
    return hadoop;
  }

  public boolean getZookeepers() {
    return zookeeper;
  }

  public boolean getLibJars() {
    return libJars;
  }
}
