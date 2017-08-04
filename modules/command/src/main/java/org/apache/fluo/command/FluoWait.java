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

import java.io.File;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FluoWait {

  private static final Logger log = LoggerFactory.getLogger(FluoWait.class);
  private static final long MIN_SLEEP_SEC = 10;
  private static final long MAX_SLEEP_SEC = 300;

  private static long calculateSleep(long notifyCount) {
    long sleep = notifyCount / 500;
    if (sleep < MIN_SLEEP_SEC) {
      return MIN_SLEEP_SEC;
    } else if (sleep > MAX_SLEEP_SEC) {
      return MAX_SLEEP_SEC;
    }
    return sleep;
  }

  @VisibleForTesting
  public static long countNotifications(Environment env) {
    Scanner scanner = null;
    try {
      scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
    } catch (TableNotFoundException e) {
      log.error("An exception was thrown -", e);
      throw new FluoException(e);
    }

    Notification.configureScanner(scanner);

    return Iterables.size(scanner);
  }

  public static void waitUntilFinished(FluoConfiguration config) {
    try (Environment env = new Environment(config)) {
      log.info("The wait command will exit when all notifications are processed");
      while (true) {
        long ts1 = env.getSharedResources().getOracleClient().getStamp().getTxTimestamp();
        long ntfyCount = countNotifications(env);
        long ts2 = env.getSharedResources().getOracleClient().getStamp().getTxTimestamp();
        if (ntfyCount == 0 && ts1 == (ts2 - 1)) {
          log.info("All processing has finished!");
          break;
        }

        try {
          long sleepSec = calculateSleep(ntfyCount);
          log.info("{} notifications are still outstanding.  Will try again in {} seconds...",
              ntfyCount, sleepSec);
          Thread.sleep(1000 * sleepSec);
        } catch (InterruptedException e) {
          log.error("Sleep was interrupted!  Exiting...");
          System.exit(-1);
        }
      }
    } catch (FluoException e) {
      log.error(e.getMessage());
      System.exit(-1);
    } catch (Exception e) {
      log.error("An exception was thrown -", e);
      System.exit(-1);
    }
  }


  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: FluoWait <connectionPropsPath> <applicationName>");
      System.exit(-1);
    }
    String connectionPropsPath = args[0];
    String applicationName = args[1];
    Objects.requireNonNull(connectionPropsPath);
    File connectionPropsFile = new File(connectionPropsPath);
    Preconditions.checkArgument(connectionPropsFile.exists(), connectionPropsPath
        + " does not exist");

    FluoConfiguration fluoConfig = new FluoConfiguration(connectionPropsFile);
    fluoConfig.setApplicationName(applicationName);
    CommandUtil.verifyAppRunning(fluoConfig);
    fluoConfig = FluoAdminImpl.mergeZookeeperConfig(fluoConfig);

    waitUntilFinished(fluoConfig);
  }
}
