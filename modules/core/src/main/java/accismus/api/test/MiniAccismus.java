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
package accismus.api.test;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.Scanner;

import accismus.api.config.WorkerProperties;
import accismus.impl.ByteUtil;
import accismus.impl.Configuration;
import accismus.impl.Constants;
import accismus.impl.OracleServer;
import accismus.impl.WorkerTask;

/**
 * 
 */
public class MiniAccismus {
  private OracleServer oserver;
  private Configuration aconfig;
  private ExecutorService tp;
  private AtomicBoolean shutdownFlag;

  private int numProcessing = 0;

  private class MiniWorkerTask extends WorkerTask {

    public MiniWorkerTask(Configuration config, AtomicBoolean shutdownFlag) {
      super(config, shutdownFlag);
    }

    @Override
    public void startedProcessing() {
      synchronized (MiniAccismus.this) {
        numProcessing++;
      }
    }

    @Override
    public void finishedProcessing(long numProcessed) {
      synchronized (MiniAccismus.this) {
        numProcessing--;
      }
    }
  }

  private synchronized boolean isProcessing(Scanner scanner) {
    return scanner.iterator().hasNext() || numProcessing > 0;
  }

  public MiniAccismus(Properties props) {
    try {
      aconfig = new Configuration(props);
      shutdownFlag = new AtomicBoolean(false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void start() {
    // TODO check if already started
    try {
      oserver = new OracleServer(aconfig);
      oserver.start();

      int numThreads = Integer.parseInt(aconfig.getWorkerProperties().getProperty(WorkerProperties.NUM_THREADS_PROP));

      tp = Executors.newFixedThreadPool(numThreads);
      for (int i = 0; i < numThreads; i++) {
        tp.submit(new MiniWorkerTask(aconfig, shutdownFlag));
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
        aconfig.getSharedResources().close();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void waitForObservers() {
    try {
      Scanner scanner = aconfig.getConnector().createScanner(aconfig.getTable(), aconfig.getAuthorizations());
      scanner.fetchColumnFamily(ByteUtil.toText(Constants.NOTIFY_CF));

      while (isProcessing(scanner)) {
        Thread.sleep(100);
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
