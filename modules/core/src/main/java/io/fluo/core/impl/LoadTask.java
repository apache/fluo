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

package io.fluo.core.impl;

import io.fluo.api.client.Loader;
import io.fluo.api.client.Transaction;
import io.fluo.api.exceptions.CommitException;
import io.fluo.core.log.TracingTransaction;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 */
public class LoadTask implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(LoadTask.class);
  private final Loader loader;
  private final Environment env;

  public LoadTask(Loader loader, Environment env) {
    this.loader = loader;
    this.env = env;
  }

  @Override
  public void run() {
    while (true) {
      TransactionImpl txi = null;
      Transaction tx = null;
      String status = "UNKNOWN";
      try {
        txi = new TransactionImpl(env);
        tx = txi;
        if (TracingTransaction.isTracingEnabled()) {
          tx = new TracingTransaction(txi, loader.getClass());
        }
        Loader.Context context = new Loader.Context() {
          @Override
          public Configuration getAppConfiguration() {
            return env.getAppConfiguration();
          }
        };
        loader.load(tx, context);
        tx.commit();
        status = "COMMITTED";
        return;
      } catch (CommitException e) {
        status = "COMMIT_EXCEPTION";
        // retry
      } catch (Exception e) {
        status = "ERROR";
        log.warn("Failed to execute loader " + loader.getClass().getSimpleName(), e);
        throw new RuntimeException(e);
      } finally {
        if (txi != null) {
          try {
            txi.getStats().report(env.getMeticNames(), status, loader.getClass(),
                env.getSharedResources().getMetricRegistry());
          } finally {
            tx.close();
          }
        }
      }
    }
  }

}
