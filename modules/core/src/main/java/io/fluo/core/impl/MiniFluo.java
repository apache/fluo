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
package io.fluo.core.impl;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.fluo.api.config.WorkerProperties;
import io.fluo.api.data.impl.ByteUtil;
import io.fluo.core.oracle.OracleServer;
import io.fluo.core.util.ColumnUtil;
import org.apache.accumulo.core.client.Scanner;

/**
 * 
 */
public class MiniFluo {
  private OracleServer oserver;
  private Environment env;
  private ExecutorService tp;
  private AtomicBoolean shutdownFlag;

  private int numProcessing = 0;

  private class MiniWorkerTask extends WorkerTask {

    public MiniWorkerTask(Environment env, AtomicBoolean shutdownFlag) {
      super(env, shutdownFlag);
    }

    @Override
    public void startedProcessing() {
      synchronized (MiniFluo.this) {
        numProcessing++;
      }
    }

    @Override
    public void finishedProcessing(long numProcessed) {
      synchronized (MiniFluo.this) {
        numProcessing--;
      }
    }
  }

  private synchronized boolean isProcessing(Scanner scanner) {
    return scanner.iterator().hasNext() || numProcessing > 0;
  }

  public MiniFluo(Properties props) {
    try {
      env = new Environment(props);
      shutdownFlag = new AtomicBoolean(false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void start() {
    // TODO check if already started
    try {
      oserver = new OracleServer(env);
      oserver.start();

      int numThreads = Integer.parseInt(env.getWorkerProperties().getProperty(WorkerProperties.WORKER_NUM_THREADS_PROP));

      tp = Executors.newFixedThreadPool(numThreads);
      for (int i = 0; i < numThreads; i++) {
        tp.submit(new MiniWorkerTask(env, shutdownFlag));
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void stop() {

    try {
      if (oserver != null) {
        oserver.stop();
        shutdownFlag.set(true);
        tp.shutdownNow();
        while (!tp.awaitTermination(1, TimeUnit.SECONDS)) {

        }
        env.getSharedResources().close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void waitForObservers() {
    try {
      Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
      scanner.fetchColumnFamily(ByteUtil.toText(ColumnUtil.NOTIFY_CF));

      while (isProcessing(scanner)) {
        Thread.sleep(100);
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
