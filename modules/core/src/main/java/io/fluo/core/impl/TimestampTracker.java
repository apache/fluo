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

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fluo.accumulo.util.LongUtil;
import io.fluo.accumulo.util.ZookeeperConstants;
import io.fluo.core.oracle.OracleClient;
import io.fluo.core.util.CuratorUtil;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allocates timestamps from Oracle for transactions and tracks 
 * the oldest active timestamp in Zookeeper for garbage collection
 */
public class TimestampTracker implements AutoCloseable {

  public static enum TrackerStatus { IDLE, BUSY, CLOSED };

  private static final Logger log = LoggerFactory.getLogger(TimestampTracker.class);
  private long zkTimestamp = -1;
  private final Environment env;
  private SortedSet<Long> timestamps = new TreeSet<>();
  private PersistentEphemeralNode node = null;
  private final TransactorID tid;
  private TrackerStatus status;  // tracks status of object and not zookeeper
  private final Timer timer;

  public TimestampTracker(Environment env, TransactorID tid, long updatePeriodMs) {
    Preconditions.checkNotNull(env, "environment cannot null");
    Preconditions.checkNotNull(tid, "tid cannot null");
    Preconditions.checkArgument(updatePeriodMs > 0, "update period must be positive");
    this.env = env;
    this.tid = tid;
    status = TrackerStatus.IDLE;
    TimerTask tt = new TimerTask() {
      @Override
      public void run() {
        try {
          boolean update = false;
          long ts = 0;

          synchronized(TimestampTracker.this) {
            if (status == TrackerStatus.CLOSED) {
              return;
            } else if (status == TrackerStatus.BUSY) {
              update = true;
              ts = getOldestActiveTimestamp();
            } else if (status == TrackerStatus.IDLE) {
              closeZkNode();
            } else {
              log.error("Unknown status encountered in Zookeeper update thread");
              return;
            }
          }

          // update can be done outside of sync block as timer has one thread and future
          // executions of run method will block until this method returns
          if (update) {
            updateZkNode(ts);
          }
        } catch (Exception e) {
          log.error("Exception occurred in Zookeeper update thread", e);
        }
      }
    };
    timer = new Timer("TimestampTracker timer", true);
    timer.schedule(tt, updatePeriodMs, updatePeriodMs);
  }

  public TimestampTracker(Environment env, TransactorID tid) {
    this(env, tid, ZookeeperConstants.ZK_UPDATE_PERIOD_MS);
  }

  /**
   * Allocate a timestamp
   */
  public synchronized long allocateTimestamp() {
    // initialize zookeeper if idle and node does not exist
    Preconditions.checkState(status != TrackerStatus.CLOSED, "tracker is closed");
    if ((status == TrackerStatus.IDLE) && (node == null)) {
      // initialize zookeeper node first with a timestamp
      long initialTs = getTimestamp();
      if (node == null) {
        updateZkNode(initialTs);
      }
    } 

    // retrieve timestamp to return
    long ts = getTimestamp();

    // update timestamp set and status
    timestamps.add(ts);
    status = TrackerStatus.BUSY;

    return ts;
  }

  /**
   * Remove a timestamp (of completed transaction)
   */
  public synchronized void removeTimestamp(long ts) throws NoSuchElementException {
    Preconditions.checkState(status == TrackerStatus.BUSY, "tracker should be busy");
    Preconditions.checkNotNull(node);
    if (timestamps.remove(ts) == false) {
      throw new NoSuchElementException("Timestamp "+ts+" was previously removed or does not exist");
    }
    if (timestamps.isEmpty()) {
      status = TrackerStatus.IDLE;
    }
  }

  private long getTimestamp() {
    try {
      return OracleClient.getInstance(env).getTimestamp();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private void updateZkNode(long ts) {
    if (node == null) {
      node = new PersistentEphemeralNode(env.getSharedResources().getCurator(), 
          Mode.EPHEMERAL, getNodePath(), LongUtil.toByteArray(ts));
      CuratorUtil.startAndWait(node, 10);
    } else if (ts != zkTimestamp) {
      try {
        node.setData(LongUtil.toByteArray(ts));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
    zkTimestamp = ts;
  }

  private void closeZkNode() {
    try {
      if (node != null) {
        node.close();
        node = null;
      }
    } catch (IOException e) {
      log.error("Failed to close timestamp tracker ephemeral node");
      throw new IllegalStateException(e);
    }
  }

  @VisibleForTesting
  synchronized void updateZkNode() {
    if (status == TrackerStatus.BUSY) {
      updateZkNode(getOldestActiveTimestamp());
    } else if (status == TrackerStatus.IDLE) {
      closeZkNode();
    }
  }

  @VisibleForTesting
  long getOldestActiveTimestamp() {
    return timestamps.first();
  }

  @VisibleForTesting
  long getZookeeperTimestamp() {
    return zkTimestamp;
  }

  @VisibleForTesting
  boolean isEmpty() {
    return timestamps.isEmpty();
  }

  @VisibleForTesting
  String getNodePath() {
    return ZookeeperConstants.transactorTsRoot(env.getZookeeperRoot()) + "/" + tid.toString();
  }

  @Override
  public synchronized void close() {
    Preconditions.checkState(status != TrackerStatus.CLOSED, "tracker already closed");
    status = TrackerStatus.CLOSED;
    timer.cancel();
    closeZkNode();
  }
}
