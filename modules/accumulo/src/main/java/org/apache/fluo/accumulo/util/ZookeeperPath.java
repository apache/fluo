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

package org.apache.fluo.accumulo.util;

/**
 * Common Fluo Zookeeper Paths
 */
public class ZookeeperPath {

  public static final String CONFIG = "/config";
  public static final String CONFIG_ACCUMULO_TABLE = CONFIG + "/accumulo.table";
  public static final String CONFIG_ACCUMULO_INSTANCE_NAME = CONFIG + "/accumulo.instance.name";
  public static final String CONFIG_ACCUMULO_INSTANCE_ID = CONFIG + "/accumulo.instance.id";
  public static final String CONFIG_FLUO_APPLICATION_ID = CONFIG + "/fluo.application.id";
  public static final String CONFIG_FLUO_OBSERVERS = CONFIG + "/fluo.observers";
  public static final String CONFIG_SHARED = CONFIG + "/shared.config";

  public static final String ORACLE = "/oracle";
  public static final String ORACLE_MAX_TIMESTAMP = ORACLE + "/max-timestamp";
  public static final String ORACLE_GC_TIMESTAMP = ORACLE + "/gc-timestamp";
  public static final String ORACLE_SERVER = ORACLE + "/server";

  public static final String TRANSACTOR = "/transactor";
  public static final String TRANSACTOR_COUNT = TRANSACTOR + "/count";
  public static final String TRANSACTOR_NODES = TRANSACTOR + "/nodes";
  public static final String TRANSACTOR_TIMESTAMPS = TRANSACTOR + "/timestamps";

  public static final String YARN = "/yarn";
  public static final String YARN_TWILL_ID = YARN + "/twill.id";
  public static final String YARN_APP_ID = YARN + "/app.id";

  public static final String TWILL = "/twill";
  public static final String FINDERS = "/finders";

}
