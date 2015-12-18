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

package io.fluo.core.impl;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.core.metrics.MetricNames;
import io.fluo.core.metrics.MetricsUtil;

public class TxStats {
  private final long startTime;
  private long lockWaitTime = 0;
  private long entriesReturned = 0;
  private long entriesSet = 0;
  private long finishTime = 0;
  private long collisions = -1;
  // number of entries recovered from other transactions
  private long recovered = 0;
  private long deadLocks = 0;
  private long timedOutLocks = 0;
  private Map<Bytes, Set<Column>> rejected = Collections.emptyMap();
  private long commitTs = -1;
  private final Environment env;
  private long precommitTime = -1;
  private long commitPrimaryTime = -1;
  private long finishCommitTime = -1;

  TxStats(Environment env) {
    this.startTime = System.currentTimeMillis();
    this.env = env;
  }

  public long getLockWaitTime() {
    return lockWaitTime;
  }

  public long getEntriesReturned() {
    return entriesReturned;
  }

  public long getEntriesSet() {
    return entriesSet;
  }

  public long getTime() {
    return finishTime - startTime;
  }

  public long getPrecommitTime() {
    return precommitTime;
  }

  public long getCommitPrimaryTime() {
    return commitPrimaryTime;
  }

  public long getFinishCommitTime() {
    return finishCommitTime;
  }

  public long getCollisions() {
    if (collisions == -1) {
      collisions = 0;
      for (Set<Column> cols : rejected.values()) {
        collisions += cols.size();
      }
    }
    return collisions;
  }

  public long getRecovered() {
    return recovered;
  }

  public long getDeadLocks() {
    return deadLocks;
  }

  public long getTimedOutLocks() {
    return timedOutLocks;
  }

  public Map<Bytes, Set<Column>> getRejected() {
    return rejected;
  }

  public long getCommitTs() {
    return commitTs;
  }

  public void setCommitTs(long ts) {
    this.commitTs = ts;
  }

  void incrementLockWaitTime(long l) {
    lockWaitTime += l;
  }

  void incrementEntriesReturned(long l) {
    entriesReturned += l;
  }

  void incrementEntriesSet(long l) {
    entriesSet += l;
  }

  void incrementCollisions(long c) {
    collisions += c;
  }

  public void setRejected(Map<Bytes, Set<Column>> rejected) {
    Preconditions.checkNotNull(rejected);
    Preconditions.checkState(this.rejected.size() == 0);
    this.rejected = rejected;
    this.collisions = -1;
  }

  void incrementDeadLocks() {
    deadLocks++;
  }

  void incrementTimedOutLocks() {
    timedOutLocks++;
  }

  void incrementTimedOutLocks(int amt) {
    timedOutLocks += amt;
  }

  void setFinishTime(long t) {
    finishTime = t;
  }

  public void report(String status, Class<?> execClass) {
    MetricNames names = env.getMetricNames();
    MetricRegistry registry = env.getSharedResources().getMetricRegistry();
    String sn = execClass.getSimpleName();
    if (getLockWaitTime() > 0) {
      MetricsUtil.getTimer(env.getConfiguration(), registry, names.getTxLockWaitTime() + sn)
          .update(getLockWaitTime(), TimeUnit.MILLISECONDS);
    }
    MetricsUtil.getTimer(env.getConfiguration(), registry, names.getTxExecTime() + sn).update(
        getTime(), TimeUnit.MILLISECONDS);
    if (getCollisions() > 0) {
      registry.meter(names.getTxWithCollision() + sn).mark();
      registry.meter(names.getTxCollisions() + sn).mark(getCollisions());
    }
    registry.meter(names.getTxEntriesSet() + sn).mark(getEntriesSet());
    registry.meter(names.getTxEntriesRead() + sn).mark(getEntriesReturned());
    if (getTimedOutLocks() > 0) {
      registry.meter(names.getTxLocksTimedout() + sn).mark(getTimedOutLocks());
    }
    if (getDeadLocks() > 0) {
      registry.meter(names.getTxLocksDead() + sn).mark(getDeadLocks());
    }
    registry.meter(names.getTxStatus() + status.toLowerCase() + "." + sn).mark();
  }

  public void setCommitTimes(long precommitTime, long commitPrimaryTime, long finishCommitTime) {
    this.precommitTime = precommitTime;
    this.commitPrimaryTime = commitPrimaryTime;
    this.finishCommitTime = finishCommitTime;
  }
}
