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

package io.fluo.core.worker;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.exceptions.CommitException;
import io.fluo.api.observer.Observer;
import io.fluo.core.exceptions.AlreadyAcknowledgedException;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.TracingTransaction;
import io.fluo.core.impl.TransactionImpl;
import io.fluo.core.impl.TxLogger;

public class WorkTask implements Runnable {

  private Environment env;
  private Bytes row;
  private Column col;
  private Observers observers;
  private NotificationFinder notificationFinder;
  
  WorkTask(NotificationFinder notificationFinder, Environment env, Bytes row, Column col, Observers observers){
    this.notificationFinder = notificationFinder;
    this.env = env;
    this.row = row;
    this.col = col;
    this.observers = observers;
  }
  
  @Override
  public void run() {
    TransactionImpl txi = null;
    Observer observer = observers.getObserver(col);
    try {
      while (true) {
        TxResult status = TxResult.UNKNOWN;
        try {
          txi = new TransactionImpl(env, row, col);
          TransactionBase tx = txi;
          if (TracingTransaction.isTracingEnabled())
            tx = new TracingTransaction(tx);

          observer.process(tx, row, col);
          txi.commit();
          status = TxResult.COMMITTED;
          break;
        } catch (AlreadyAcknowledgedException aae) {
          status = TxResult.AACKED;
          notificationFinder.failedToProcess(row, col, status);
          break;
        } catch (CommitException e) {
          // retry
          status = TxResult.COMMIT_EXCEPTION;
        } catch (Exception e) {
          status = TxResult.ERROR;
          notificationFinder.failedToProcess(row, col, status);
          break;
        } finally {
          if (txi != null) {
            txi.getStats().report(status.toString(), observer.getClass(), env.getSharedResources().getMetricRegistry());
            if (TxLogger.isLoggingEnabled())
              TxLogger.logTx(status.toString(), observer.getClass().getSimpleName(), txi.getStats(), row + ":" + col);
          }
        }
        // TODO if duplicate set detected, see if its because already acknowledged
      }
    } finally {
      // close after multiple commit attempts
      if (txi != null) {
        txi.close();
      }
      
      observers.returnObserver(observer);
    }
    
  }

}
