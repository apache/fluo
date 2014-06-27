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
package org.fluo.yarn;

import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.twill.api.AbstractTwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.fluo.api.config.WorkerProperties;
import org.fluo.cluster.util.Logging;
import org.fluo.impl.Configuration;
import org.fluo.impl.WorkerTask;
import org.fluo.tools.InitializeTool;

import com.beust.jcommander.JCommander;

/** Main run method of Fluo worker that can be called within
 * a Twill/YARN application or on its own as a Java application
 */
public class WorkerRunnable extends AbstractTwillRunnable {

  private static Logger log = LoggerFactory.getLogger(WorkerRunnable.class);
  
  @Override
  public void run() {
    String[] args = { "-config-dir", "./conf"};
    run(args);
  }
  
  public void run(String[] args) {
    try {
      RunnableOptions options = new RunnableOptions();
      JCommander jcommand = new JCommander(options, args);

      if (options.help) {
        jcommand.usage();
        System.exit(-1);
      }
      options.validateConfig();

      Logging.init("worker", options.getConfigDir(), options.getLogOutput());

      Configuration config = new Configuration(InitializeTool.loadProps(options.getFluoConfig()));

      for (Entry<Object,Object> entry : config.getWorkerProperties().entrySet()) {
        log.info("config " + entry.getKey() + " = " + entry.getValue());
      }

      int numThreads = Integer.parseInt(config.getWorkerProperties().getProperty(WorkerProperties.NUM_THREADS_PROP));

      ExecutorService tp = Executors.newFixedThreadPool(numThreads);
      for (int i = 0; i < numThreads; i++) {
        tp.submit(new WorkerTask(config, new AtomicBoolean(false)));
      }

      // TODO push work onto a queue for each notification found instead of having each thread scan for notifications.

      while (true)
        UtilWaitThread.sleep(1000);

    } catch (Exception e) {
      System.err.println("Exception running worker: "+ e.getMessage());
      e.printStackTrace();
    }
  }
  
  @Override
  public void stop() {
    log.info("Stopping Fluo worker");
  }

  public static void main(String[] args) throws Exception {
    WorkerRunnable worker = new WorkerRunnable();
    worker.run(args);
  }
}
