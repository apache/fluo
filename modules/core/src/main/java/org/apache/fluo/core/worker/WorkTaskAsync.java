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

package org.apache.fluo.core.worker;

import org.apache.fluo.api.observer.Observer;
import org.apache.fluo.core.async.AsyncCommitObserver;
import org.apache.fluo.core.async.AsyncTransaction;
import org.apache.fluo.core.async.CommitManager;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.impl.TransactionImpl;
import org.apache.fluo.core.log.TracingTransaction;
import org.apache.fluo.core.observer.Observers;
import org.apache.fluo.core.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkTaskAsync implements Runnable {

  private static Logger log = LoggerFactory.getLogger(WorkTaskAsync.class);

  private Environment env;
  private Notification notification;
  private Observers observers;
  private NotificationFinder notificationFinder;

  private NotificationProcessor notificationProcessor;

  class WorkTaskCommitObserver implements AsyncCommitObserver {

    @Override
    public void committed() {
      notificationProcessor.notificationProcessed(notification);
    }

    @Override
    public void failed(Throwable t) {
      notificationFinder.failedToProcess(notification, TxResult.ERROR);
      notificationProcessor.notificationProcessed(notification);
      log.error("Failed to process work " + Hex.encNonAscii(notification), t);
    }

    @Override
    public void alreadyAcknowledged() {
      notificationFinder.failedToProcess(notification, TxResult.AACKED);
      notificationProcessor.notificationProcessed(notification);
    }

    @Override
    public void commitFailed(String msg) {
      notificationProcessor.requeueNotification(notificationFinder, notification);
    }
  }

  WorkTaskAsync(NotificationProcessor notificationProcessor, NotificationFinder notificationFinder,
      Environment env, Notification notification, Observers observers) {
    this.notificationProcessor = notificationProcessor;
    this.notificationFinder = notificationFinder;
    this.env = env;
    this.notification = notification;
    this.observers = observers;
  }

  @Override
  public void run() {
    Observer observer = observers.getObserver(notification.getColumn());
    String observerId = observers.getObserverId(notification.getColumn());
    try {
      AsyncTransaction atx = new TransactionImpl(env, notification);

      if (TracingTransaction.isTracingEnabled()) {
        atx = new TracingTransaction(atx, notification, observer.getClass(), observerId);
      }

      try {
        observer.process(atx, notification.getRow(), notification.getColumn());
      } catch (Exception e) {
        notificationFinder.failedToProcess(notification, TxResult.ERROR);
        notificationProcessor.notificationProcessed(notification);
        throw e;
      }

      CommitManager commitManager = env.getSharedResources().getCommitManager();
      commitManager.beginCommit(atx, observerId, new WorkTaskCommitObserver());

    } catch (Exception e) {
      log.error("Failed to process work " + Hex.encNonAscii(notification), e);
    } finally {
      observers.returnObserver(observer);
    }
  }
}
