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
package io.fluo.core.client;

import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.client.Snapshot;
import io.fluo.api.client.Transaction;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.TransactionImpl;
import io.fluo.core.metrics.ReporterUtil;
import org.apache.commons.configuration.Configuration;
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

  public static final AutoCloseable setupReporters(Environment env, String id, AtomicInteger reporterCounter) {
    return ReporterUtil.setupReporters(env, FluoConfiguration.FLUO_PREFIX + "." + id + "." + reporterCounter.getAndIncrement());
  }
  
  public FluoClientImpl(FluoConfiguration config) {
    this.config = config;
    if (!config.hasRequiredClientProps()) {
      throw new IllegalArgumentException("Client configuration is missing required properties");
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
      return new LoaderExecutorImpl(config, env);
    } catch (Exception e) {
      log.error("Failed to create a LoaderExecutor");
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Snapshot newSnapshot() {
    return new TransactionImpl(env);
  }

  @Override
  public Transaction newTransaction() {
    return new TransactionImpl(env);
  }

  @Override
  public Configuration getAppConfiguration() {
    return env.getAppConfiguration();
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
