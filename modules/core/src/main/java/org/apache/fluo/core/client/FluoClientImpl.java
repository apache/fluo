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

package org.apache.fluo.core.client;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.TransactionImpl;
import org.apache.fluo.core.log.TracingTransaction;
import org.apache.fluo.core.metrics.ReporterUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Fluo Client
 */
public class FluoClientImpl implements FluoClient {

  private static final Logger log = LoggerFactory.getLogger(FluoClientImpl.class);
  private static final AtomicInteger reporterCounter = new AtomicInteger(1);

  private FluoConfiguration config;
  private Environment env;
  private AutoCloseable reporter;

  public static final AutoCloseable setupReporters(Environment env, String id,
      AtomicInteger reporterCounter) {
    return ReporterUtil.setupReporters(env, FluoConfiguration.FLUO_PREFIX + "." + id + "."
        + reporterCounter.getAndIncrement());
  }

  public FluoClientImpl(FluoConfiguration config) {
    this.config = config;
    if (!config.hasRequiredClientProps()) {
      String msg = "Client configuration is missing required properties";
      log.error(msg);
      throw new IllegalArgumentException(msg);
    }
    try {
      this.env = new Environment(config);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }

    reporter = setupReporters(env, "client", reporterCounter);
  }

  @Override
  public LoaderExecutor newLoaderExecutor() {
    try {
      return new LoaderExecutorAsyncImpl(config, env);
    } catch (Exception e) {
      log.error("Failed to create a LoaderExecutor");
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Snapshot newSnapshot() {
    TransactionImpl tx = new TransactionImpl(env);
    if (TracingTransaction.isTracingEnabled()) {
      return new TracingTransaction(tx);
    }
    return tx;
  }

  @Override
  public Transaction newTransaction() {
    TransactionImpl tx = new TransactionImpl(env) {
      @Override
      public void commit() {
        super.commit();
        // wait for any async mutations that transaction write to flush
        env.getSharedResources().getBatchWriter().waitForAsyncFlush();
      }
    };
    if (TracingTransaction.isTracingEnabled()) {
      return new TracingTransaction(tx);
    }
    return tx;
  }

  @Override
  public SimpleConfiguration getAppConfiguration() {
    return env.getAppConfiguration();
  }

  @Override
  public MetricsReporter getMetricsReporter() {
    return env.getMetricsReporter();
  }

  @Override
  public void close() {
    env.close();
    try {
      reporter.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
