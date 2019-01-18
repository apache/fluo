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

package org.apache.fluo.command;

import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.fluo.core.worker.finder.hash.TableRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MINUTES;

public class FluoWait {

  private static final Logger log = LoggerFactory.getLogger(FluoWait.class);
  private static final long MIN_SLEEP_MS = 250;
  private static final long MAX_SLEEP_MS = MINUTES.toMillis(5);

  private static List<TableRange> getRanges(Environment env)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    List<TableRange> ranges =
        TableRange.fromTexts(env.getAccumuloClient().tableOperations().listSplits(env.getTable()));
    Collections.shuffle(ranges);
    return ranges;
  }

  private static boolean hasNotifications(Environment env, TableRange range)
      throws TableNotFoundException {
    try (Scanner scanner =
        env.getAccumuloClient().createScanner(env.getTable(), env.getAuthorizations())) {
      scanner.setRange(range.getRange());
      Notification.configureScanner(scanner);

      return scanner.iterator().hasNext();
    }
  }

  /**
   * Wait until a range has no notifications.
   *
   * @return true if notifications were ever seen while waiting
   */
  private static boolean waitTillNoNotifications(Environment env, TableRange range)
      throws TableNotFoundException {
    boolean sawNotifications = false;
    long retryTime = MIN_SLEEP_MS;

    log.debug("Scanning tablet {} for notifications", range);

    long start = System.currentTimeMillis();
    while (hasNotifications(env, range)) {
      sawNotifications = true;
      long sleepTime = Math.max(System.currentTimeMillis() - start, retryTime);
      log.debug("Tablet {} had notfications, will rescan in {}ms", range, sleepTime);
      UtilWaitThread.sleep(sleepTime);
      retryTime = Math.min(MAX_SLEEP_MS, (long) (retryTime * 1.5));
      start = System.currentTimeMillis();
    }

    return sawNotifications;
  }

  /**
   * Wait until a scan of the table completes without seeing notifications AND without the Oracle
   * issuing any timestamps during the scan.
   */
  private static void waitUntilFinished(FluoConfiguration config) {
    try (Environment env = new Environment(config)) {
      List<TableRange> ranges = getRanges(env);

      outer: while (true) {
        long ts1 = env.getSharedResources().getOracleClient().getStamp().getTxTimestamp();
        for (TableRange range : ranges) {
          boolean sawNotifications = waitTillNoNotifications(env, range);
          if (sawNotifications) {
            ranges = getRanges(env);
            // This range had notifications. Processing those notifications may have created
            // notifications in previously scanned ranges, so start over.
            continue outer;
          }
        }
        long ts2 = env.getSharedResources().getOracleClient().getStamp().getTxTimestamp();

        // Check to ensure the Oracle issued no timestamps during the scan for notifications.
        if (ts2 - ts1 == 1) {
          break;
        }
      }
    } catch (Exception e) {
      log.error("An exception was thrown -", e);
      System.exit(-1);
    }
  }

  public static void main(String[] args) throws Exception {
    CommonOpts opts = CommonOpts.parse("fluo wait", args);
    FluoConfiguration config = CommandUtil.resolveFluoConfig();
    config.setApplicationName(opts.getApplicationName());
    opts.overrideFluoConfig(config);
    CommandUtil.verifyAppRunning(config);
    config = FluoAdminImpl.mergeZookeeperConfig(config);
    waitUntilFinished(config);
  }
}
