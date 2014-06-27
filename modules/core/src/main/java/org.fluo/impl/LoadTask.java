/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fluo.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.fluo.api.Loader;
import org.fluo.api.Transaction;
import org.fluo.api.exceptions.CommitException;

/**
 * 
 */
public class LoadTask implements Runnable {
  
  private static Logger log = LoggerFactory.getLogger(LoadTask.class);
  private Loader loader;
  private Configuration config;
  
  public LoadTask(Loader loader, Configuration config) {
    this.loader = loader;
    this.config = config;
  }
  
  @Override
  public void run() {
    while (true) {
      TransactionImpl txi = null;
      String status = "FAILED";
      try {
        txi = new TransactionImpl(config);
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
