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

  public static final String METRICS_REPORTER_ID_PROP =
      FluoConfiguration.FLUO_PREFIX + ".metrics.reporter.id";

  // Metrics prefixes for 'default' metrics
  public static final String CLASS_PREFIX = FluoConfiguration.FLUO_PREFIX + ".class";
  public static final String SYSTEM_PREFIX = FluoConfiguration.FLUO_PREFIX + ".system";

  // Metrics prefix for 'application' metrics
  public static final String APPLICATION_PREFIX = FluoConfiguration.FLUO_PREFIX + ".app";

  private final String txLockWaitTime;
  private final String txExecTime;
  private final String txWithCollision;
  private final String txCollisions;
  private final String txEntriesSet;
  private final String txEntriesRead;
  private final String txLocksTimedOut;
  private final String txLocksDead;
  private final String txStatusPrefix;
  private final String txCommitting;

  private final String notificationsQueued;

  private final String oracleResponseTime;
  private final String oracleClientStamps;
  private final String oracleServerStamps;

  public MetricNames(String metricsReporterId, String appName) {
    Preconditions.checkArgument(!appName.contains("."),
        "Fluo App name should not contain '.': " + appName);
    Preconditions.checkArgument(!metricsReporterId.contains("."),
        "Metrics Reporter ID should not contain '.': " + metricsReporterId);

    // Metrics reported for a specific class
    // FORMAT: fluo.class.APPLICATION.REPORTER_ID.METRIC.CLASS
    final String classMetric = CLASS_PREFIX + "." + appName + "." + metricsReporterId + ".";
    txLockWaitTime = classMetric + "tx_lock_wait_time";
    txExecTime = classMetric + "tx_execution_time";
    txWithCollision = classMetric + "tx_with_collision";
    txCollisions = classMetric + "tx_collisions";
    txEntriesSet = classMetric + "tx_entries_set";
    txEntriesRead = classMetric + "tx_entries_read";
    txLocksTimedOut = classMetric + "tx_locks_timedout";
    txLocksDead = classMetric + "tx_locks_dead";
    txStatusPrefix = classMetric + "tx_status_"; // status appended to metric name

    // System-wide metrics
    // FORMAT: fluo.system.APPLICATION.REPORTER_ID.METRIC
    final String systemMetric = SYSTEM_PREFIX + "." + appName + "." + metricsReporterId + ".";
    txCommitting = systemMetric + "transactor_committing";
    notificationsQueued = systemMetric + "worker_notifications_queued";
    oracleResponseTime = systemMetric + "oracle_response_time";
    oracleClientStamps = systemMetric + "oracle_client_stamps";
    oracleServerStamps = systemMetric + "oracle_server_stamps";
  }

  public String getTxLockWaitTime(String className) {
    return txLockWaitTime + "." + className;
  }

  public String getTxExecTime(String className) {
    return txExecTime + "." + className;
  }

  public String getTxWithCollision(String className) {
    return txWithCollision + "." + className;
  }

  public String getTxCollisions(String className) {
    return txCollisions + "." + className;
  }

  public String getTxEntriesSet(String className) {
    return txEntriesSet + "." + className;
  }

  public String getTxEntriesRead(String className) {
    return txEntriesRead + "." + className;
  }

  public String getTxLocksTimedout(String className) {
    return txLocksTimedOut + "." + className;
  }

  public String getTxLocksDead(String className) {
    return txLocksDead + "." + className;
  }

  public String getTxStatus(String status, String className) {
    return txStatusPrefix + status + "." + className;
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
