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

package org.apache.fluo.core.impl;

import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.config.FluoConfiguration;

/**
 * Contains implementation-related Fluo properties that should not be exposed in the API in
 * {@link FluoConfiguration}
 */
public class FluoConfigurationImpl {

  public static final String FLUO_IMPL_PREFIX = FluoConfiguration.FLUO_PREFIX + ".impl";

  public static final String ORACLE_PORT_PROP = FLUO_IMPL_PREFIX + ".oracle.port";
  public static final String WORKER_FINDER_PROP = FLUO_IMPL_PREFIX + ".worker.finder";
  public static final String WORKER_PARTITION_GROUP_SIZE =
      FLUO_IMPL_PREFIX + ".worker.finder.partition.groupSize";
  public static final int WORKER_PARTITION_GROUP_SIZE_DEFAULT = 7;
  public static final String METRICS_RESERVOIR_PROP = FLUO_IMPL_PREFIX + ".metrics.reservoir";
  public static final String NTFY_FINDER_MIN_SLEEP_TIME_PROP =
      FLUO_IMPL_PREFIX + ".worker.finder.minSleep";
  public static final int NTFY_FINDER_MIN_SLEEP_TIME_DEFAULT = 5000;
  public static final String NTFY_FINDER_MAX_SLEEP_TIME_PROP =
      FLUO_IMPL_PREFIX + ".worker.finder.maxSleep";
  public static final int NTFY_FINDER_MAX_SLEEP_TIME_DEFAULT = 5 * 60 * 1000;

  public static final String ACCUMULO_JARS_REGEX_PROP = FLUO_IMPL_PREFIX + ".accumulo.jars.regex";
  public static final String ACCUMULO_JARS_REGEX_DEFAULT = "^fluo-(api|accumulo).*";

  // Time period that each client will update ZK with their oldest active timestamp
  // If period is too short, Zookeeper may be overloaded. If too long, garbage collection
  // may keep older versions of table data unnecessarily.
  public static final String ZK_UPDATE_PERIOD_PROP = FLUO_IMPL_PREFIX + ".timestamp.update.period";
  public static long ZK_UPDATE_PERIOD_MS_DEFAULT = 60000;

  // CW is short for ConditionalWriter
  public static final String CW_MIN_THREADS_PROP = FLUO_IMPL_PREFIX + ".cw.threads.min";
  public static final int CW_MIN_THREADS_DEFAULT = 3;
  public static final String CW_MAX_THREADS_PROP = FLUO_IMPL_PREFIX + ".cw.threads.max";
  public static final int CW_MAX_THREADS_DEFAULT = 20;

  public static int getNumCWThreads(FluoConfiguration conf, int numTservers) {
    int min = conf.getInt(CW_MIN_THREADS_PROP, CW_MIN_THREADS_DEFAULT);
    int max = conf.getInt(CW_MAX_THREADS_PROP, CW_MAX_THREADS_DEFAULT);

    if (min < 0 || max < 0 || min > max) {
      throw new IllegalArgumentException("Bad conditional writer thread props " + min + " " + max);
    }

    int numThreads = numTservers;
    numThreads = Math.min(numThreads, max);
    numThreads = Math.max(numThreads, min);

    return numThreads;
  }

  // BW is short for BatchWriter
  public static final String BW_MIN_THREADS_PROP = FLUO_IMPL_PREFIX + ".bw.threads.min";
  public static final int BW_MIN_THREADS_DEFAULT = 3;
  public static final String BW_MAX_THREADS_PROP = FLUO_IMPL_PREFIX + ".bw.threads.max";
  public static final int BW_MAX_THREADS_DEFAULT = 20;

  public static int getNumBWThreads(FluoConfiguration conf, int numTservers) {
    int min = conf.getInt(BW_MIN_THREADS_PROP, BW_MIN_THREADS_DEFAULT);
    int max = conf.getInt(BW_MAX_THREADS_PROP, BW_MAX_THREADS_DEFAULT);

    if (min < 0 || max < 0 || min > max) {
      throw new IllegalArgumentException("Bad batch writer thread props " + min + " " + max);
    }

    int numThreads = numTservers;
    numThreads = Math.min(numThreads, max);
    numThreads = Math.max(numThreads, min);

    return numThreads;
  }

  // max memory to buffer committing transactions.. when this is full submitting transactions for
  // commit will wait
  public static final String COMMIT_MEMORY_PROP = FLUO_IMPL_PREFIX + ".tx.commit.memory";
  public static final int COMMIT_MEMORY_DEFAULT = 20 * 1024 * 1024;

  public static int getTxCommitMemory(FluoConfiguration conf) {
    int m = conf.getInt(COMMIT_MEMORY_PROP, COMMIT_MEMORY_DEFAULT);
    if (m <= 0) {
      throw new IllegalArgumentException("Bad value for " + COMMIT_MEMORY_PROP + " " + m);
    }
    return m;
  }

  public static final String TX_INFO_CACHE_SIZE = FLUO_IMPL_PREFIX + ".tx.failed.cache.size.mb";
  public static final long TX_INFO_CACHE_SIZE_DEFAULT = 10_000_000;

  /** 
   * Gets the txinfo cache size
   * 
   * @param conf The FluoConfiguration
   * @return The size of the cache value from the property value {@value #TX_INFO_CACHE_SIZE}
   *     if it is set, else the value of the default value {@value #TX_INFO_CACHE_SIZE_DEFAULT}
   */

  public static long getTxInfoCacheSize(FluoConfiguration conf) {
    long size = conf.getLong(TX_INFO_CACHE_SIZE, TX_INFO_CACHE_SIZE_DEFAULT);
    if (size <= 0) {
      throw new IllegalArgumentException("Cache size must be positive for " + TX_INFO_CACHE_SIZE);
    }
    return size;
  }

  public static final String TX_INFO_CACHE_TIMEOUT =
      FLUO_IMPL_PREFIX + ".tx.failed.cache.expireTime.ms";
  public static final long TX_INFO_CACHE_TIMEOUT_DEFAULT = 24 * 60 * 1000;

  /**
   * Gets the time before stale entries in the cache are evicted based on age.
   * This method returns a long representing the time converted from the
   * TimeUnit passed in.
   * 
   * @param conf The FluoConfiguration
   * @param tu   The TimeUnit desired to represent the cache timeout
   */

  public static long getTxIfoCacheTimeout(FluoConfiguration conf, TimeUnit tu) {
    long millis = conf.getLong(TX_INFO_CACHE_TIMEOUT, TX_INFO_CACHE_TIMEOUT_DEFAULT);
    if (millis <= 0) {
      throw new IllegalArgumentException("Timeout must positive for " + TX_INFO_CACHE_TIMEOUT);
    }
    return tu.convert(millis, TimeUnit.MILLISECONDS);
  }

  public static final String VISIBILITY_CACHE_SIZE = FLUO_IMPL_PREFIX + ".visibility.cache.size.mb";
  public static final long VISIBILITY_CACHE_SIZE_DEFAULT = 10_000_000;

  /** 
   * Gets the visibility cache size
   * 
   * @param conf The FluoConfiguration
   * @return The size of the cache value from the property value {@value #VISIBILITY_CACHE_SIZE}
   *     if it is set, else the value of the default value {@value #VISIBILITY_CACHE_SIZE_DEFAULT}
   */

  public static long getVisibilityCacheSize(FluoConfiguration conf) {
    long size = conf.getLong(VISIBILITY_CACHE_SIZE, VISIBILITY_CACHE_SIZE_DEFAULT);
    if (size <= 0) {
      throw new IllegalArgumentException(
          "Cache size must be positive for " + VISIBILITY_CACHE_SIZE);
    }
    return size;
  }

  public static final String VISIBILITY_CACHE_TIMEOUT =
      FLUO_IMPL_PREFIX + ".visibility.cache.expireTime.ms";
  public static final long VISIBILITY_CACHE_TIMEOUT_DEFAULT = 24 * 60 * 1000;

  /**
   * Gets the time before stale entries in the cache are evicted based on age.
   * This method returns a long representing the time converted from the
   * TimeUnit passed in.
   * 
   * @param conf The FluoConfiguration
   * @param tu   The TimeUnit desired to represent the cache timeout
   */

  public static long getVisibilityCacheTimeout(FluoConfiguration conf, TimeUnit tu) {
    long millis = conf.getLong(VISIBILITY_CACHE_TIMEOUT, VISIBILITY_CACHE_TIMEOUT_DEFAULT);
    if (millis <= 0) {
      throw new IllegalArgumentException("Timeout must positive for " + VISIBILITY_CACHE_TIMEOUT);
    }
    return tu.convert(millis, TimeUnit.MILLISECONDS);
  }

  private static final String TRANSACTOR_MAX_CACHE_SIZE =
      FLUO_IMPL_PREFIX + ".transactor.cache.max.size";
  private static final long TRANSACTOR_MAX_CACHE_SIZE_DEFAULT = 32768; // this equals 2^15 

  public static long getTransactorMaxCacheSize(FluoConfiguration conf) {
    long size = conf.getLong(TRANSACTOR_MAX_CACHE_SIZE, TRANSACTOR_MAX_CACHE_SIZE_DEFAULT);
    if (size <= 0) {
      throw new IllegalArgumentException(
          "Cache size must be positive for " + TRANSACTOR_MAX_CACHE_SIZE);
    }
    return size;
  }

  public static final String TRANSACTOR_CACHE_SIZE = FLUO_IMPL_PREFIX + ".transactor.cache.size.mb";
  public static final long TRANSACTOR_CACHE_SIZE_DEFAULT = 10_000_000;

  /** 
   * Gets the transactor cache size
   * 
   * @param conf The FluoConfiguration
   * @return The size of the cache value from the property value {@value #TRANSACTOR_CACHE_SIZE}
   *     if it is set, else the value of the default value {@value #TRANSACTOR_CACHE_SIZE_DEFAULT}
   */

  public static long getTransactorCacheSize(FluoConfiguration conf) {
    long size = conf.getLong(TRANSACTOR_CACHE_SIZE, TRANSACTOR_CACHE_SIZE_DEFAULT);
    if (size <= 0) {
      throw new IllegalArgumentException(
          "Cache size must be positive for " + TRANSACTOR_CACHE_SIZE);
    }
    return size;
  }

  public static final String TRANSACTOR_CACHE_TIMEOUT =
      FLUO_IMPL_PREFIX + ".transactor.cache.expireTime.ms";

  public static final long TRANSACTOR_CACHE_TIMEOUT_DEFAULT = 24 * 60 * 1000;

  public static long getTransactorCacheTimeout(FluoConfiguration conf, TimeUnit tu) {
    long millis = conf.getLong(TRANSACTOR_CACHE_TIMEOUT, TRANSACTOR_CACHE_TIMEOUT_DEFAULT);
    if (millis <= 0) {
      throw new IllegalArgumentException("Timeout must positive for " + TRANSACTOR_CACHE_TIMEOUT);
    }
    return tu.convert(millis, TimeUnit.MILLISECONDS);
  }

  public static final String ASYNC_CW_THREADS = FLUO_IMPL_PREFIX + ".async.cw.threads";
  public static final int ASYNC_CW_THREADS_DEFAULT = 8;
  public static final String ASYNC_CW_LIMIT = FLUO_IMPL_PREFIX + ".async.cw.limit";
  public static final int ASYNC_CW_LIMIT_DEFAULT = 100000;

  public static final String ASYNC_COMMIT_THREADS = FLUO_IMPL_PREFIX + ".tx.commit.threads.async";
  public static final int ASYNC_COMMIT_THREADS_DEFAULT = 8;

  public static final String SYNC_COMMIT_THREADS = FLUO_IMPL_PREFIX + ".tx.commit.threads.sync";
  public static final int SYNC_COMMIT_THREADS_DEFAULT = 32;
}
