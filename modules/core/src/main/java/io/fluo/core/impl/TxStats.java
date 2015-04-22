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

  TxStats() {
    this.startTime = System.currentTimeMillis();
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

  public void report(MetricNames names, String status, Class<?> execClass, MetricRegistry registry) {
    String sn = execClass.getSimpleName();
    if (getLockWaitTime() > 0) {
      registry.timer(names.getTxLockwait() + sn).update(getLockWaitTime(), TimeUnit.MILLISECONDS);
    }
    registry.timer(names.getTxTime() + sn).update(getTime(), TimeUnit.MILLISECONDS);
    if (getCollisions() > 0) {
      registry.histogram(names.getTxCollisions() + sn).update(getCollisions());
    }
    registry.histogram(names.getTxSet() + sn).update(getEntriesSet());
    registry.histogram(names.getTxRead() + sn).update(getEntriesReturned());
    if (getTimedOutLocks() > 0) {
      registry.histogram(names.getTxLocksTimedout() + sn).update(getTimedOutLocks());
    }
    if (getDeadLocks() > 0) {
      registry.histogram(names.getTxLocksDead() + sn).update(getDeadLocks());
    }
    registry.counter(names.getTxStatus() + status.toLowerCase() + "." + sn).inc();
  }
}
