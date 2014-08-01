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
package io.fluo.api.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

/**
 * The class helps create a properties object with the key/values required to connect to a Fluo instance.
 */
public class ConnectionProperties extends Properties {
  
  private static final long serialVersionUID = 1L;
  
  public static final String FLUO_PREFIX = "io.fluo";

  public static final String ACCUMULO_PASSWORD_PROP = FLUO_PREFIX + ".accumulo.password";
  public static final String ACCUMULO_USER_PROP = FLUO_PREFIX + ".accumulo.user";
  public static final String ACCUMULO_INSTANCE_PROP = FLUO_PREFIX + ".accumulo.instance";

  public static final String ZOOKEEPER_ROOT_PROP = FLUO_PREFIX + ".zookeeper.root";
  public static final String ZOOKEEPER_TIMEOUT_PROP = FLUO_PREFIX + ".zookeeper.timeout";
  public static final String ZOOKEEPER_CONNECT_PROP = FLUO_PREFIX + ".zookeeper.connect";
  
  public static final String CLIENT_CLASS_PROP = FLUO_PREFIX + ".client.class";
  public static final String DEFAULT_CLIENT_CLASS = FLUO_PREFIX + ".core.client.FluoClientImpl";
  
  public ConnectionProperties() {
    super(getDefaultProperties());
  }
  
  public ConnectionProperties(File file) throws FileNotFoundException, IOException {
    this();
    load(new FileInputStream(file));
  }

  public ConnectionProperties(Properties props) {
    super(props);
    Set<java.util.Map.Entry<Object,Object>> es = getDefaultProperties().entrySet();
    for (java.util.Map.Entry<Object,Object> entry : es) {
      setDefault((String) entry.getKey(), (String) entry.getValue());
    }
  }

  public ConnectionProperties setZookeepers(String zookeepers) {
    setProperty(ZOOKEEPER_CONNECT_PROP, zookeepers);
    return this;
  }
  
  public ConnectionProperties setTimeout(int timeout) {
    setProperty(ZOOKEEPER_TIMEOUT_PROP, timeout + "");
    return this;
  }
  
  public ConnectionProperties setZookeeperRoot(String zookeeperRoot) {
    setProperty(ZOOKEEPER_ROOT_PROP, zookeeperRoot);
    return this;
  }
  
  public ConnectionProperties setAccumuloInstance(String accumuloInstance) {
    setProperty(ACCUMULO_INSTANCE_PROP, accumuloInstance);
    return this;
  }
  
  public ConnectionProperties setAccumuloUser(String accumuloUser) {
    setProperty(ACCUMULO_USER_PROP, accumuloUser);
    return this;
  }
  
  public ConnectionProperties setAccumuloPassword(String accumuloPassword) {
    setProperty(ACCUMULO_PASSWORD_PROP, accumuloPassword);
    return this;
  }
    
  protected void setDefault(String key, String val) {
    if (getProperty(key) == null)
      setProperty(key, val);
  }
  
  public static Properties getDefaultProperties() {
    Properties props = new Properties();
    setDefaultProperties(props);
    return props;
  }
  
  public static void setDefaultProperties(Properties props) {
      props.put(ZOOKEEPER_CONNECT_PROP, "localhost");
      props.put(ZOOKEEPER_ROOT_PROP, "/fluo");
      props.put(ZOOKEEPER_TIMEOUT_PROP, "30000");
      props.put(ACCUMULO_INSTANCE_PROP, "accumulo1");
      props.put(ACCUMULO_USER_PROP, "fluo");
      props.put(ACCUMULO_PASSWORD_PROP, "secret"); 
      props.put(CLIENT_CLASS_PROP, DEFAULT_CLIENT_CLASS);
  }
}
