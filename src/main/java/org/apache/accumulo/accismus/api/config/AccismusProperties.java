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
package org.apache.accumulo.accismus.api.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * The class helps create a properties object with the key/values required to connect to an Accismus instance.
 */
public class AccismusProperties extends Properties {
  
  private static final long serialVersionUID = 1L;

  public static final String ACCUMULO_PASSWORD_PROP = "accismus.accumulo.password";
  public static final String ACCUMULO_USER_PROP = "accismus.accumulo.user";
  public static final String ACCUMULO_INSTANCE_PROP = "accismus.accumulo.instance";
  public static final String ZOOKEEPER_ROOT_PROP = "accismus.zookeeper.root";
  public static final String ZOOKEEPER_TIMEOUT_PROP = "accismus.zookeeper.timeout";
  public static final String ZOOKEEPER_CONNECT_PROP = "accismus.zookeeper.connect";
  
  public AccismusProperties() {
    super(org.apache.accumulo.accismus.impl.Configuration.getDefaultProperties());
  }
  
  public AccismusProperties(File file) throws FileNotFoundException, IOException {
    this();
    load(new FileInputStream(file));
  }

  public AccismusProperties(Properties props) {
    super(props);
    Set<java.util.Map.Entry<Object,Object>> es = org.apache.accumulo.accismus.impl.Configuration.getDefaultProperties().entrySet();
    for (java.util.Map.Entry<Object,Object> entry : es) {
      setDefault((String) entry.getKey(), (String) entry.getValue());
    }
  }

  public AccismusProperties setZookeepers(String zookeepers) {
    setProperty(ZOOKEEPER_CONNECT_PROP, zookeepers);
    return this;
  }
  
  public AccismusProperties setTimeout(int timeout) {
    setProperty(ZOOKEEPER_TIMEOUT_PROP, timeout + "");
    return this;
  }
  
  public AccismusProperties setZookeeperRoot(String zookeeperRoot) {
    setProperty(ZOOKEEPER_ROOT_PROP, zookeeperRoot);
    return this;
  }
  
  public AccismusProperties setAccumuloInstance(String accumuloInstance) {
    setProperty(ACCUMULO_INSTANCE_PROP, accumuloInstance);
    return this;
  }
  
  public AccismusProperties setAccumuloUser(String accumuloUser) {
    setProperty(ACCUMULO_USER_PROP, accumuloUser);
    return this;
  }
  
  public AccismusProperties setAccumuloPassword(String accumuloPassword) {
    setProperty(ACCUMULO_PASSWORD_PROP, accumuloPassword);
    return this;
  }

  protected void setDefault(String key, String val) {
    if (getProperty(key) == null)
      setProperty(key, val);
  }

}
