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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.worker.NotificationFinder;
import org.apache.fluo.core.worker.NotificationProcessor;
import org.apache.fluo.core.worker.TxResult;

public class PartitionNotificationFinder implements NotificationFinder {

  private PartitionManager partitionManager;
  private Thread scanThread;
  private NotificationProcessor processor;
  private Environment env;
  private AtomicBoolean stopped;

  @Override
  public void init(Environment env, NotificationProcessor processor) {
    this.processor = processor;
    this.env = env;
    this.stopped = new AtomicBoolean(false);

  }

  @Override
  public void start() {
    long minSleepTime =
        env.getConfiguration().getInt(FluoConfigurationImpl.NTFY_FINDER_MIN_SLEEP_TIME_PROP,
            FluoConfigurationImpl.NTFY_FINDER_MIN_SLEEP_TIME_DEFAULT);
    long maxSleepTime =
        env.getConfiguration().getInt(FluoConfigurationImpl.NTFY_FINDER_MAX_SLEEP_TIME_PROP,
            FluoConfigurationImpl.NTFY_FINDER_MAX_SLEEP_TIME_DEFAULT);

    partitionManager = new PartitionManager(env, minSleepTime, maxSleepTime);

    scanThread = new Thread(
        new ScanTask(this, processor, partitionManager, env, stopped, minSleepTime, maxSleepTime));
    scanThread.setName(getClass().getSimpleName() + " " + ScanTask.class.getSimpleName());
    scanThread.setDaemon(true);
    scanThread.start();
  }

  @Override
  public void stop() {
    stopped.set(true);

    scanThread.interrupt();
    try {
      scanThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    partitionManager.stop();
  }

  @Override
  public boolean shouldProcess(Notification notification) {
    return partitionManager.shouldProcess(notification);
  }

  @Override
  public void failedToProcess(Notification notification, TxResult status) {}

}
