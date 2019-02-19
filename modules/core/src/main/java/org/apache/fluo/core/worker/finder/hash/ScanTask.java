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

package org.apache.fluo.core.worker.finder.hash;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.iterators.NotificationHashFilter;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.fluo.core.worker.NotificationFinder;
import org.apache.fluo.core.worker.NotificationProcessor;
import org.apache.fluo.core.worker.NotificationProcessor.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(ScanTask.class);

  private final NotificationFinder finder;
  private final PartitionManager partitionManager;
  private final NotificationProcessor proccessor;
  private final Random rand = new Random();
  private final AtomicBoolean stopped;
  private final Map<TableRange, TabletData> rangeData;
  private final Environment env;

  private long minSleepTime;
  private long maxSleepTime;

  private static final Map<String, String> SCAN_EXEC_HINTS =
      Collections.singletonMap("scan_type", "fluo-ntfy");

  ScanTask(NotificationFinder finder, NotificationProcessor proccessor,
      PartitionManager partitionManager, Environment env, AtomicBoolean stopped, long minSleepTime,
      long maxSleepTime) {
    this.finder = finder;
    this.rangeData = new HashMap<>();

    this.env = env;
    this.stopped = stopped;

    this.proccessor = proccessor;
    this.partitionManager = partitionManager;

    this.minSleepTime = minSleepTime;
    this.maxSleepTime = maxSleepTime;
  }

  @Override
  public void run() {

    List<TableRange> ranges = new ArrayList<>();
    Set<TableRange> rangeSet = new HashSet<>();

    int qSize = proccessor.size();

    while (!stopped.get()) {
      try {
        ranges.clear();
        rangeSet.clear();

        PartitionInfo partition = partitionManager.waitForPartitionInfo();

        while (proccessor.size() > qSize / 2 && !stopped.get()) {
          UtilWaitThread.sleep(50, stopped);
        }

        partition.getMyGroupsRanges().forEach(t -> {
          ranges.add(t);
          rangeSet.add(t);
        });
        Collections.shuffle(ranges, rand);
        rangeData.keySet().retainAll(rangeSet);

        long minRetryTime = maxSleepTime + System.currentTimeMillis();
        ScanCounts ntfyCounts = new ScanCounts();
        int tabletsScanned = 0;
        try {
          for (TableRange tabletRange : ranges) {
            TabletData tabletData = rangeData.computeIfAbsent(tabletRange, tr -> new TabletData());
            if (System.currentTimeMillis() >= tabletData.retryTime) {
              ScanCounts counts;
              PartitionInfo pi = partitionManager.getPartitionInfo();
              if (partition.equals(pi)) {
                try (Session session =
                    proccessor.beginAddingNotifications(rc -> tabletRange.contains(rc.getRow()))) {
                  // notifications could have been asynchronously queued for deletion. Let that
                  // happen 1st before scanning
                  env.getSharedResources().getBatchWriter().waitForAsyncFlush();

                  counts = scan(session, partition, tabletRange.getRange());
                  tabletsScanned++;
                }
              } else {
                break;
              }
              tabletData.updateScanCount(counts.added, maxSleepTime);
              ntfyCounts.added += counts.added;
              ntfyCounts.seen += counts.seen;
              if (stopped.get()) {
                break;
              }
            }

            minRetryTime = Math.min(tabletData.retryTime, minRetryTime);
          }
        } catch (PartitionInfoChangedException mpce) {
          // nothing to do
        }

        long sleepTime;
        if (!partition.equals(partitionManager.getPartitionInfo())) {
          sleepTime = minSleepTime;
        } else {
          sleepTime = Math.max(minSleepTime, minRetryTime - System.currentTimeMillis());
        }

        qSize = proccessor.size();

        log.debug("Scanned {} of {} tablets. Notifications added: {} seen: {} queued: {}",
            tabletsScanned, ranges.size(), ntfyCounts.added, ntfyCounts.seen, qSize);

        if (!stopped.get()) {
          UtilWaitThread.sleep(sleepTime, stopped);
        }

      } catch (Exception e) {
        if (isInterruptedException(e)) {
          log.debug("Error while looking for notifications", e);
        } else {
          log.error("Error while looking for notifications", e);
        }
      }
    }
  }

  private boolean isInterruptedException(Exception e) {
    boolean wasInt = false;
    Throwable cause = e;
    while (cause != null) {
      if (cause instanceof InterruptedException) {
        wasInt = true;
      }
      cause = cause.getCause();
    }
    return wasInt;
  }

  private static class ScanCounts {
    int seen = 0;
    int added = 0;
  }

  private ScanCounts scan(Session session, PartitionInfo pi, Range range)
      throws TableNotFoundException {
    try (Scanner scanner =
        env.getAccumuloClient().createScanner(env.getTable(), env.getAuthorizations())) {

      scanner.setRange(range);

      Notification.configureScanner(scanner);

      IteratorSetting iterCfg = new IteratorSetting(30, "nhf", NotificationHashFilter.class);
      NotificationHashFilter.setModulusParams(iterCfg, pi.getMyGroupSize(), pi.getMyIdInGroup());
      scanner.addScanIterator(iterCfg);

      scanner.setExecutionHints(SCAN_EXEC_HINTS);

      ScanCounts counts = new ScanCounts();

      for (Entry<Key, Value> entry : scanner) {
        if (!pi.equals(partitionManager.getPartitionInfo())) {
          throw new PartitionInfoChangedException();
        }

        if (stopped.get()) {
          return counts;
        }

        counts.seen++;

        if (session.addNotification(finder, Notification.from(entry.getKey()))) {
          counts.added++;
        }
      }
      return counts;
    }
  }
}
