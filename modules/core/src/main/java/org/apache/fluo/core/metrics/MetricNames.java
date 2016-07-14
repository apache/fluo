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

package org.apache.fluo.core.metrics;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.config.FluoConfiguration;

public class MetricNames {

  private final String txLockWaitTime;
  private final String txExecTime;
  private final String txWithCollision;
  private final String txCollisions;
  private final String txEntriesSet;
  private final String txEntriesRead;
  private final String txLocksTimedOut;
  private final String txLocksDead;
  private final String txStatus;
  private final String txCommitting;

  private final String notificationsQueued;

  private final String oracleResponseTime;
  private final String oracleClientStamps;
  private final String oracleServerStamps;

  public static final String METRICS_ID_PROP = FluoConfiguration.FLUO_PREFIX + ".metrics.id";

  public MetricNames(String hostId, String appName) {
    Preconditions.checkArgument(!appName.contains("."), "Fluo App name should not contain '.': "
        + appName);
    Preconditions.checkArgument(!hostId.contains("."), "Host ID should not contain '.': " + hostId);

    // All metrics start with prefix "fluo.APP.HOST."
    final String metricsPrefix = FluoConfiguration.FLUO_PREFIX + "." + appName + "." + hostId + ".";

    // Transaction metrics: fluo.APP.HOST.tx.METRIC.OBSERVER
    final String txPrefix = metricsPrefix + "tx.";
    txLockWaitTime = txPrefix + "lock_wait_time.";
    txExecTime = txPrefix + "execution_time.";
    txWithCollision = txPrefix + "with_collision.";
    txCollisions = txPrefix + "collisions.";
    txEntriesSet = txPrefix + "entries_set.";
    txEntriesRead = txPrefix + "entries_read.";
    txLocksTimedOut = txPrefix + "locks_timedout.";
    txLocksDead = txPrefix + "locks_dead.";
    txStatus = txPrefix + "status_";

    txCommitting = metricsPrefix + "transactor.committing";

    // Worker metrics: fluo.APP.HOST.worker.METRIC
    notificationsQueued = metricsPrefix + "worker.notifications_queued";

    // Oracle metrics: fluo.APP.HOST.oracle.METRIC
    final String oraclePrefix = metricsPrefix + "oracle.";
    oracleResponseTime = oraclePrefix + "response_time";
    oracleClientStamps = oraclePrefix + "client_stamps";
    oracleServerStamps = oraclePrefix + "server_stamps";
  }

  public String getTxLockWaitTime() {
    return txLockWaitTime;
  }

  public String getTxExecTime() {
    return txExecTime;
  }

  public String getTxWithCollision() {
    return txWithCollision;
  }

  public String getTxCollisions() {
    return txCollisions;
  }

  public String getTxEntriesSet() {
    return txEntriesSet;
  }

  public String getTxEntriesRead() {
    return txEntriesRead;
  }

  public String getTxLocksTimedout() {
    return txLocksTimedOut;
  }

  public String getTxLocksDead() {
    return txLocksDead;
  }

  public String getTxStatus() {
    return txStatus;
  }

  public String getNotificationQueued() {
    return notificationsQueued;
  }

  public String getOracleResponseTime() {
    return oracleResponseTime;
  }

  public String getOracleClientStamps() {
    return oracleClientStamps;
  }

  public String getOracleServerStamps() {
    return oracleServerStamps;
  }

  public String getCommitsProcessing() {
    return txCommitting;
  }
}
