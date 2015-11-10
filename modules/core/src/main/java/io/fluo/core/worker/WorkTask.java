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

package io.fluo.core.worker;

import java.util.Random;

import io.fluo.api.client.Transaction;
import io.fluo.api.exceptions.CommitException;
import io.fluo.api.observer.Observer;
import io.fluo.core.exceptions.AlreadyAcknowledgedException;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.Notification;
import io.fluo.core.impl.TransactionImpl;
import io.fluo.core.log.TracingTransaction;
import io.fluo.core.util.Hex;
import io.fluo.core.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkTask implements Runnable {

  private static Logger log = LoggerFactory.getLogger(WorkTask.class);

  private Environment env;
  private Notification notification;
  private Observers observers;
  private NotificationFinder notificationFinder;

  WorkTask(NotificationFinder notificationFinder, Environment env, Notification notification,
      Observers observers) {
    this.notificationFinder = notificationFinder;
    this.env = env;
    this.notification = notification;
    this.observers = observers;
  }

  @Override
  public void run() {
    Observer observer = observers.getObserver(notification.getColumn());
    try {
      int sleepTime = 0;
      Random rand = null;

      while (true) {
        TransactionImpl txi = null;
        Transaction tx = txi;
        TxResult status = TxResult.UNKNOWN;
        try {
          UtilWaitThread.sleep(sleepTime);

          txi = new TransactionImpl(env, notification);
          tx = txi;
          if (TracingTransaction.isTracingEnabled()) {
            tx = new TracingTransaction(txi, notification, observer.getClass());
          }

          observer.process(tx, notification.getRow(), notification.getColumn());
          tx.commit();
          status = TxResult.COMMITTED;
          break;
        } catch (AlreadyAcknowledgedException aae) {
          status = TxResult.AACKED;
          notificationFinder.failedToProcess(notification, status);
          break;
        } catch (CommitException e) {
          // retry
          status = TxResult.COMMIT_EXCEPTION;
          if (sleepTime == 0) {
            rand = new Random();
            sleepTime = 10 + rand.nextInt(10);
          } else if (sleepTime < 300000) {
            sleepTime = sleepTime + rand.nextInt(sleepTime);
          }
        } catch (Exception e) {
          status = TxResult.ERROR;
          log.warn("Failed to execute observer " + observer.getClass().getSimpleName()
              + " notification : " + Hex.encNonAscii(notification), e);
          notificationFinder.failedToProcess(notification, status);
          break;
        } finally {
          if (txi != null) {
            try {
              txi.getStats().report(env.getMetricNames(), status.toString(), observer.getClass(),
                  env.getSharedResources().getMetricRegistry());
            } finally {
              tx.close();
            }
          }
        }
        // TODO if duplicate set detected, see if its because already acknowledged
      }
    } finally {
      observers.returnObserver(observer);
    }

  }

}
