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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.core.metrics.MetricNames;
import org.apache.fluo.core.metrics.MetricsUtil;

public class TxStats {
  private final long startTime;
  private long beginCommitTime;
  private long finishCommitTime;
  private long lockWaitTime = 0;
  private long entriesReturned = 0;
  private long entriesSet = 0;
  private long collisions = -1;
  // number of entries recovered from other transactions
  private long recovered = 0;
  private long deadLocks = 0;
  private long timedOutLocks = 0;
  private Map<Bytes, Set<Column>> rejected = Collections.emptyMap();
  private long commitTs = -1;
  private final Environment env;

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
    return finishCommitTime - startTime;
  }

  public long getReadTime() {
    return beginCommitTime - startTime;
  }

  public long getCommitTime() {
    return finishCommitTime - beginCommitTime;
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
    Objects.requireNonNull(rejected);
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

  public void report(String status, String alias) {
    MetricNames names = env.getMetricNames();
    MetricRegistry registry = env.getSharedResources().getMetricRegistry();
    if (getLockWaitTime() > 0) {
      MetricsUtil.getTimer(env.getConfiguration(), registry, names.getTxLockWaitTime(alias))
          .update(getLockWaitTime(), TimeUnit.MILLISECONDS);
    }
    MetricsUtil.getTimer(env.getConfiguration(), registry, names.getTxExecTime(alias)).update(
        getReadTime(), TimeUnit.MILLISECONDS);
    if (getCollisions() > 0) {
      registry.meter(names.getTxWithCollision(alias)).mark();
      registry.meter(names.getTxCollisions(alias)).mark(getCollisions());
    }
    registry.meter(names.getTxEntriesSet(alias)).mark(getEntriesSet());
    registry.meter(names.getTxEntriesRead(alias)).mark(getEntriesReturned());
    if (getTimedOutLocks() > 0) {
      registry.meter(names.getTxLocksTimedout(alias)).mark(getTimedOutLocks());
    }
    if (getDeadLocks() > 0) {
      registry.meter(names.getTxLocksDead(alias)).mark(getDeadLocks());
    }
    registry.meter(names.getTxStatus(status.toLowerCase(), alias)).mark();
  }

  public void setCommitBeginTime(long t) {
    this.beginCommitTime = t;
  }

  public void setCommitFinishTime(long t) {
    this.finishCommitTime = t;
  }
}
