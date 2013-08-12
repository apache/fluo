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
package org.apache.accumulo.accismus.api;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.accismus.impl.Configuration;
import org.apache.accumulo.accismus.impl.Constants.Props;

/**
 * 
 */
public class AccismusProperties extends Properties {
  
  private static final long serialVersionUID = 1L;
  
  public AccismusProperties(File file) throws FileNotFoundException, IOException {
    super(Configuration.getDefaultProperties());
    load(new FileInputStream(file));
  }

  /**
   * @param zooKeepers
   * @param timeout
   * @param zookeeperRoot
   * @param accumuloInstance
   * @param accumuloUser
   * @param accumuloPassword
   */
  public AccismusProperties(String zooKeepers, int timeout, String zookeeperRoot, String accumuloInstance, String accumuloUser, String accumuloPassword) {
    super(Configuration.getDefaultProperties());
    setProperty(Props.ZOOKEEPER_CONNECT, zooKeepers);
    setProperty(Props.ZOOKEEPER_TIMEOUT, timeout + "");
    setProperty(Props.ZOOKEEPER_ROOT, zookeeperRoot);
    
    setProperty(Props.ACCUMULO_INSTANCE, accumuloInstance);
    setProperty(Props.ACCUMULO_USER, accumuloUser);
    setProperty(Props.ACCUMULO_PASSWORD, accumuloPassword);
  }

}
