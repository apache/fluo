/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.core.metrics;

import io.fluo.api.config.FluoConfiguration;

public class MetricNames {

  private final String txLockWait;
  private final String txTime;
  private final String txCollisions;
  private final String txSet;
  private final String txRead;
  private final String txLocksTimedOut;
  private final String txLocksDead;
  private final String txStatus;

  private final String notificationsQueued;

  private final String oracleClientStamps;
  private final String oracleClientRpc;
  private final String oracleServerStamps;


  public static final String METRICS_ID_PROP = FluoConfiguration.FLUO_PREFIX + ".metrics.id";

  public MetricNames(String hostId) {
    txLockWait = FluoConfiguration.FLUO_PREFIX + "." + hostId + ".tx.lockWait.";
    txTime = FluoConfiguration.FLUO_PREFIX + "." + hostId + ".tx.time.";
    txCollisions = FluoConfiguration.FLUO_PREFIX + "." + hostId + ".tx.collisions.";
    txSet = FluoConfiguration.FLUO_PREFIX + "." + hostId + ".tx.set.";
    txRead = FluoConfiguration.FLUO_PREFIX + "." + hostId + ".tx.read.";
    txLocksTimedOut = FluoConfiguration.FLUO_PREFIX + "." + hostId + ".tx.locks.timedout.";
    txLocksDead = FluoConfiguration.FLUO_PREFIX + "." + hostId + ".tx.locks.dead.";
    txStatus = FluoConfiguration.FLUO_PREFIX + "." + hostId + ".tx.status.";

    notificationsQueued =
        FluoConfiguration.FLUO_PREFIX + "." + hostId + ".worker.notifications.queued";

    oracleClientStamps = FluoConfiguration.FLUO_PREFIX + "." + hostId + ".oracle.client.stamps";
    oracleClientRpc =
        FluoConfiguration.FLUO_PREFIX + "." + hostId + ".oracle.client.rpc.getStamps.time";
    oracleServerStamps = FluoConfiguration.FLUO_PREFIX + "." + hostId + ".oracle.server.stamps";
  }

  public String getTxLockwait() {
    return txLockWait;
  }

  public String getTxTime() {
    return txTime;
  }

  public String getTxCollisions() {
    return txCollisions;
  }

  public String getTxSet() {
    return txSet;
  }

  public String getTxRead() {
    return txRead;
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

  public String getOracleClientStamps() {
    return oracleClientStamps;
  }

  public String getOracleClientGetStamps() {
    return oracleClientRpc;
  }

  public String getOracleServerStamps() {
    return oracleServerStamps;
  }
}
