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

import io.fluo.core.exceptions.CommitException;
import io.fluo.api.client.Loader;
import io.fluo.api.client.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class LoadTask implements Runnable {
  
  private static Logger log = LoggerFactory.getLogger(LoadTask.class);
  private Loader loader;
  private Environment env;
  
  public LoadTask(Loader loader, Environment env) {
    this.loader = loader;
    this.env = env;
  }
  
  @Override
  public void run() {
    while (true) {
      TransactionImpl txi = null;
      String status = "FAILED";
      try {
        txi = new TransactionImpl(env);
        Transaction tx = txi;
        if (TracingTransaction.isTracingEnabled())
          tx = new TracingTransaction(tx);
        loader.load(tx);
        txi.commit();
        status = "COMMITTED";
        return;
      } catch (CommitException e) {
        // retry
      } catch (Exception e) {
        log.error("Failed to execute loader " + loader, e);
        throw new RuntimeException(e);
      } finally {
        if (txi != null)
          TxLogger.logTx(status, loader.getClass().getName(), txi.getStats());
      }
    }
  }
  
}
