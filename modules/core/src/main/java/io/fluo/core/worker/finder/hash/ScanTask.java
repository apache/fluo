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

package io.fluo.core.worker.finder.hash;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Supplier;
import io.fluo.accumulo.iterators.NotificationHashFilter;
import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.Notification;
import io.fluo.core.util.ByteUtil;
import io.fluo.core.util.UtilWaitThread;
import io.fluo.core.worker.TabletInfoCache;
import io.fluo.core.worker.TabletInfoCache.TabletInfo;
import io.fluo.core.worker.finder.hash.HashNotificationFinder.ModParamsChangedException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScanTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(ScanTask.class);

  private final HashNotificationFinder hwf;
  private final Random rand = new Random();
  private final AtomicBoolean stopped;
  private final TabletInfoCache<TabletData, Supplier<TabletData>> tabletInfoCache;
  private final Environment env;

  public static final String MIN_SLEEP_TIME_PROP = FluoConfiguration.FLUO_PREFIX + ".impl."
      + ScanTask.class.getSimpleName() + ".minSleep";
  public static final String MAX_SLEEP_TIME_PROP = FluoConfiguration.FLUO_PREFIX + ".impl."
      + ScanTask.class.getSimpleName() + ".maxSleep";


  static long STABALIZE_TIME = 10 * 1000;

  private long minSleepTime;
  private long maxSleepTime;

  ScanTask(HashNotificationFinder hashWorkFinder, Environment env, AtomicBoolean stopped) {
    this.hwf = hashWorkFinder;
    this.tabletInfoCache =
        new TabletInfoCache<TabletData, Supplier<TabletData>>(env, new Supplier<TabletData>() {
          @Override
          public TabletData get() {
            return new TabletData();
          }
        });
    this.env = env;
    this.stopped = stopped;

    minSleepTime = env.getConfiguration().getInt(MIN_SLEEP_TIME_PROP, 5000);
    maxSleepTime = env.getConfiguration().getInt(MAX_SLEEP_TIME_PROP, 5 * 60 * 1000);
  }

  @Override
  public void run() {

    int qSize = hwf.getWorkerQueue().size();

    while (!stopped.get()) {
      try {

        while (hwf.getWorkerQueue().size() > qSize / 2 && !stopped.get()) {
          UtilWaitThread.sleep(50, stopped);
        }

        // break scan work into a lot of ranges that are randomly ordered. This has a few benefits.
        // Ensures different workers are scanning different tablets.
        // Allows checking local state more frequently in the case where work is not present in many
        // tablets. Allows less frequent scanning of tablets that are
        // usually empty.
        List<TabletInfo<TabletData>> tablets = new ArrayList<>(tabletInfoCache.getTablets());
        Collections.shuffle(tablets, rand);

        long minRetryTime = maxSleepTime + System.currentTimeMillis();
        int notifications = 0;
        int tabletsScanned = 0;
        try {
          for (TabletInfo<TabletData> tabletInfo : tablets) {
            if (System.currentTimeMillis() >= tabletInfo.getData().retryTime) {
              int count = 0;
              ModulusParams modParams = hwf.getModulusParams();
              if (modParams != null) {
                // notifications could have been asynchronously queued for deletion. Let that happen
                // 1st before scanning
                env.getSharedResources().getBatchWriter().waitForAsyncFlush();
                count = scan(modParams, tabletInfo.getRange());
                tabletsScanned++;
              }
              tabletInfo.getData().updateScanCount(count, maxSleepTime);
              notifications += count;
              if (stopped.get()) {
                break;
              }
            }

            minRetryTime = Math.min(tabletInfo.getData().retryTime, minRetryTime);
          }
        } catch (ModParamsChangedException mpce) {
          hwf.getWorkerQueue().clear();
          waitForFindersToStabalize();
        }

        long sleepTime = Math.max(minSleepTime, minRetryTime - System.currentTimeMillis());

        qSize = hwf.getWorkerQueue().size();

        log.debug("Scanned {} of {} tablets, added {} new notifications (total queued {})",
            tabletsScanned, tablets.size(), notifications, qSize);

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

  private int scan(ModulusParams lmp, Range range) throws TableNotFoundException {
    Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());

    scanner.setRange(range);

    // table does not have versioning iterator configured, if there are multiple notification
    // versions only want to see one
    IteratorSetting iterCfg = new IteratorSetting(20, "ver", VersioningIterator.class);
    scanner.addScanIterator(iterCfg);

    iterCfg = new IteratorSetting(30, "nhf", NotificationHashFilter.class);
    NotificationHashFilter.setModulusParams(iterCfg, lmp.divisor, lmp.remainder);
    scanner.addScanIterator(iterCfg);

    scanner.fetchColumnFamily(ByteUtil.toText(ColumnConstants.NOTIFY_CF));

    int count = 0;

    for (Entry<Key, Value> entry : scanner) {
      if (lmp.update != hwf.getModulusParams().update) {
        throw new HashNotificationFinder.ModParamsChangedException();
      }

      if (stopped.get()) {
        return count;
      }

      if (hwf.getWorkerQueue().addNotification(hwf, Notification.from(entry.getKey()))) {
        count++;
      }
    }
    return count;
  }

  private void waitForFindersToStabalize() {
    ModulusParams lmp = hwf.getModulusParams();
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime < STABALIZE_TIME) {
      UtilWaitThread.sleep(500, stopped);
      ModulusParams lmp2 = hwf.getModulusParams();
      if (lmp.update != lmp2.update) {
        startTime = System.currentTimeMillis();
        lmp = lmp2;
      }
    }
  }
}
