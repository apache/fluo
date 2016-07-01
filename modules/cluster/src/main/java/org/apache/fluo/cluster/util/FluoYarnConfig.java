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

package org.apache.fluo.cluster.util;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.config.FluoConfiguration;

public class FluoYarnConfig {

  private static final String YARN_PREFIX = FluoConfiguration.FLUO_PREFIX + ".yarn";
  public static final String WORKER_INSTANCES_PROP = YARN_PREFIX + ".worker.instances";
  public static final String WORKER_MAX_MEMORY_MB_PROP = YARN_PREFIX + ".worker.max.memory.mb";
  public static final String WORKER_NUM_CORES_PROP = YARN_PREFIX + ".worker.num.cores";
  public static final int WORKER_INSTANCES_DEFAULT = 1;
  public static final int WORKER_MAX_MEMORY_MB_DEFAULT = 1024;
  public static final int WORKER_NUM_CORES_DEFAULT = 1;

  public static final String ORACLE_INSTANCES_PROP = YARN_PREFIX + ".oracle.instances";
  public static final String ORACLE_MAX_MEMORY_MB_PROP = YARN_PREFIX + ".oracle.max.memory.mb";
  public static final String ORACLE_NUM_CORES_PROP = YARN_PREFIX + ".oracle.num.cores";
  public static final int ORACLE_INSTANCES_DEFAULT = 1;
  public static final int ORACLE_MAX_MEMORY_MB_DEFAULT = 512;
  public static final int ORACLE_NUM_CORES_DEFAULT = 1;

  public static int getWorkerInstances(FluoConfiguration config) {
    return getPositiveInt(config, WORKER_INSTANCES_PROP, WORKER_INSTANCES_DEFAULT);
  }

  public static int getWorkerMaxMemory(FluoConfiguration config) {
    return getPositiveInt(config, WORKER_MAX_MEMORY_MB_PROP, WORKER_MAX_MEMORY_MB_DEFAULT);
  }

  public static int getWorkerNumCores(FluoConfiguration config) {
    return getPositiveInt(config, WORKER_NUM_CORES_PROP, WORKER_NUM_CORES_DEFAULT);
  }

  public static int getOracleMaxMemory(FluoConfiguration config) {
    return getPositiveInt(config, ORACLE_MAX_MEMORY_MB_PROP, ORACLE_MAX_MEMORY_MB_DEFAULT);
  }

  public static int getOracleInstances(FluoConfiguration config) {
    return getPositiveInt(config, ORACLE_INSTANCES_PROP, ORACLE_INSTANCES_DEFAULT);
  }

  public static int getOracleNumCores(FluoConfiguration config) {
    return getPositiveInt(config, ORACLE_NUM_CORES_PROP, ORACLE_NUM_CORES_DEFAULT);
  }

  private static int getPositiveInt(FluoConfiguration config, String property, int defaultValue) {
    int value = config.getInt(property, defaultValue);
    Preconditions.checkArgument(value > 0, property + " must be positive");
    return value;
  }
}
