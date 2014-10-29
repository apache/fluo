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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.api.client.MiniFluo;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.WorkerTask;
import io.fluo.core.oracle.OracleServer;
import io.fluo.core.util.ByteUtil;
import org.apache.accumulo.core.client.Scanner;

/**
 * Implementation of MiniFluo
 */
public class MiniFluoImpl implements MiniFluo {

  private static final AtomicInteger reporterCounter = new AtomicInteger(1);

  private final Environment env;
  private final AtomicBoolean shutdownFlag = new AtomicBoolean(false);
  private OracleServer oserver;
  private ExecutorService tp;

  private int numProcessing = 0;

  private AutoCloseable reporter;

  private class MiniWorkerTask extends WorkerTask {

    public MiniWorkerTask(Environment env, AtomicBoolean shutdownFlag) {
      super(env, shutdownFlag);
    }

    @Override
    public void startedProcessing() {
      synchronized (MiniFluoImpl.this) {
        numProcessing++;
      }
    }

    @Override
    public void finishedProcessing(long numProcessed) {
      synchronized (MiniFluoImpl.this) {
        numProcessing--;
      }
    }
  }

  private synchronized boolean isProcessing(Scanner scanner) {
    return scanner.iterator().hasNext() || numProcessing > 0;
  }

  public MiniFluoImpl(FluoConfiguration config) {
    if (!config.hasRequiredMiniFluoProps()) {
      throw new IllegalArgumentException("MiniFluo configuration is missing required properties");
    }
    try {
      env = new Environment(config);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start() {
    // TODO check if already started
    try {
      reporter = FluoClientImpl.setupReporters(env, "mini", reporterCounter);

      oserver = new OracleServer(env);
      oserver.start();

      int numThreads = env.getConfiguration().getWorkerThreads();

      tp = Executors.newFixedThreadPool(numThreads);
      for (int i = 0; i < numThreads; i++) {
        tp.submit(new MiniWorkerTask(env, shutdownFlag));
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {

    try {
      if (oserver != null) {
        oserver.stop();
        shutdownFlag.set(true);
        tp.shutdownNow();
        while (!tp.awaitTermination(1, TimeUnit.SECONDS)) {

        }
        env.close();

        reporter.close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void waitForObservers() {
    try {
      Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
      scanner.fetchColumnFamily(ByteUtil.toText(ColumnConstants.NOTIFY_CF));

      while (isProcessing(scanner)) {
        Thread.sleep(100);
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
