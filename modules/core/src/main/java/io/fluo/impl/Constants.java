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
package io.fluo.impl;

import io.fluo.api.Bytes;

/**
 * Fluo Constants
 */
public class Constants {
  public static final Bytes NOTIFY_CF = Bytes.wrap("ntfy");
  
  public static class Zookeeper {

    public static final String CONFIG = "/config";
    public static final String TABLE = CONFIG + "/accumulo.table";
    public static final String ACCUMULO_INSTANCE_NAME = CONFIG + "/accumulo.instance.name";
    public static final String ACCUMULO_INSTANCE_ID = CONFIG + "/accumulo.instance.id";
    public static final String FLUO_INSTANCE_ID = CONFIG + "/fluo.instance.id";
    public static final String OBSERVERS = CONFIG + "/fluo.observers";
    public static final String WORKER_CONFIG = CONFIG + "/fluo.workers";
    
    public static final String ORACLE = "/oracle";
    public static final String TIMESTAMP = ORACLE + "/timestamp";
    public static final String ORACLE_SERVER = ORACLE + "/server";

    public static final String TRANSACTOR = "/transactor";
    public static final String TRANSACTOR_COUNT = TRANSACTOR + "/count";
    public static final String TRANSACTOR_NODES = TRANSACTOR + "/nodes";
  }

  public static final String FLUO_PREFIX = "io.fluo";
  public static final String WORKER_THREADS = FLUO_PREFIX + ".config.worker.numThreads";
  public static final long ORACLE_MAX_READ_BUFFER_BYTES = 2048;

  public static final long ROLLBACK_TIME_DEFAULT = 300000;

  public static final String oraclePath(String zkPath) {
    return zkPath + Zookeeper.ORACLE_SERVER;
  }

  public static final String timestampPath(String zkPath) {
    return zkPath + Zookeeper.TIMESTAMP;
  }

  public static final String tablePath(String zkPath) {
    return zkPath + Zookeeper.TABLE;
  }

  public static final String configPath(String zkPath) {
    return zkPath + Zookeeper.CONFIG;
  }

  public static final String instanceNamePath(String zkPath) {
    return zkPath + Zookeeper.ACCUMULO_INSTANCE_NAME;
  }

  public static final String accumuloInstanceIdPath(String zkPath) {
    return zkPath + Zookeeper.ACCUMULO_INSTANCE_ID;
  }

  public static final String fluoInstanceIdPath(String zkPath) {
    return zkPath + Zookeeper.FLUO_INSTANCE_ID;
  }

  public static final String observersPath(String zkPath) {
    return zkPath + Constants.Zookeeper.OBSERVERS;
  }

  public static final String workerConfigPath(String zkPath) {
    return zkPath + Constants.Zookeeper.WORKER_CONFIG;
  }

}
