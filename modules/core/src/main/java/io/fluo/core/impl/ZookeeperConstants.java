/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.core.impl;

/**
 * Zookeeper Constants
 */
public class ZookeeperConstants {

  public static final String CONFIG = "/config";

  public static final String TABLE = CONFIG + "/accumulo.table";
  public static final String ACCUMULO_INSTANCE_NAME = CONFIG + "/accumulo.instance.name";
  public static final String ACCUMULO_INSTANCE_ID = CONFIG + "/accumulo.instance.id";
  public static final String FLUO_INSTANCE_ID = CONFIG + "/fluo.instance.id";
  public static final String OBSERVERS = CONFIG + "/fluo.observers";
  public static final String SHARED_CONFIG = CONFIG + "/shared.config";

  public static final String ORACLE = "/oracle";
  public static final String TIMESTAMP = ORACLE + "/timestamp";
  public static final String ORACLE_SERVER = ORACLE + "/server";

  public static final String TRANSACTOR = "/transactor";
  public static final String TRANSACTOR_COUNT = TRANSACTOR + "/count";
  public static final String TRANSACTOR_NODES = TRANSACTOR + "/nodes";

  public static final String oraclePath(String zkPath) {
    return zkPath + ORACLE_SERVER;
  }

  public static final String timestampPath(String zkPath) {
    return zkPath + TIMESTAMP;
  }

  public static final String tablePath(String zkPath) {
    return zkPath + TABLE;
  }

  public static final String configPath(String zkPath) {
    return zkPath + CONFIG;
  }

  public static final String instanceNamePath(String zkPath) {
    return zkPath + ACCUMULO_INSTANCE_NAME;
  }

  public static final String accumuloInstanceIdPath(String zkPath) {
    return zkPath + ACCUMULO_INSTANCE_ID;
  }

  public static final String fluoInstanceIdPath(String zkPath) {
    return zkPath + FLUO_INSTANCE_ID;
  }

  public static final String observersPath(String zkPath) {
    return zkPath + OBSERVERS;
  }

  public static final String sharedConfigPath(String zkPath) {
    return zkPath + SHARED_CONFIG;
  }
}
