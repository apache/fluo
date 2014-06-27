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
package org.fluo.api.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


/**
 * 
 */
public class InitializationProperties extends WorkerProperties {
  private static final long serialVersionUID = 1L;

  public static final String CLEAR_ZOOKEEPER_PROP = "org.fluo.init.zookeeper.clear";
  public static final String TABLE_PROP = "org.fluo.init.accumulo.table";
  public static final String CLASSPATH_PROP = "org.fluo.init.accumulo.classpath";

  private void setDefaults() {
    setDefault(CLEAR_ZOOKEEPER_PROP, "false");
  }

  public InitializationProperties() {
    super();
  }

  public InitializationProperties(File file) throws FileNotFoundException, IOException {
    super(file);
    setDefaults();
  }

  public InitializationProperties(Properties props) {
    super(props);
    setDefaults();
  }

  public InitializationProperties setAccumuloTable(String table) {
    setProperty(TABLE_PROP, table);
    return this;
  }

  public InitializationProperties setAccumuloClasspath(String path) {
    setProperty(CLASSPATH_PROP, path);
    return this;
  }

  public InitializationProperties setClearZookeeper(boolean clear) {
    setProperty(CLEAR_ZOOKEEPER_PROP, clear + "");
    return this;
  }

}
