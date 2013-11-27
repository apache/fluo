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
package org.apache.accumulo.accismus.api.test;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.accismus.api.config.WorkerProperties;
import org.apache.accumulo.accismus.impl.ByteUtil;
import org.apache.accumulo.accismus.impl.Configuration;
import org.apache.accumulo.accismus.impl.Constants;
import org.apache.accumulo.accismus.impl.OracleServer;
import org.apache.accumulo.accismus.impl.WorkerTask;
import org.apache.accumulo.core.client.Scanner;

/**
 * 
 */
public class MiniAccismus {
  private OracleServer oserver;
  private Configuration aconfig;
  private ExecutorService tp;
  private AtomicBoolean shutdownFlag;

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
        tp.submit(new WorkerTask(aconfig, shutdownFlag));
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
        tp.shutdown();
        while (!tp.awaitTermination(1, TimeUnit.SECONDS)) {

        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void waitForObservers() {
    // TODO create a better implementation
    try {
      Scanner scanner = aconfig.getConnector().createScanner(aconfig.getTable(), aconfig.getAuthorizations());
      scanner.fetchColumnFamily(ByteUtil.toText(Constants.NOTIFY_CF));

      while (scanner.iterator().hasNext()) {
        Thread.sleep(100);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
