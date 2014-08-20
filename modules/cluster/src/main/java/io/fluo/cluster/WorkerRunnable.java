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
package io.fluo.cluster;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.beust.jcommander.JCommander;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.cluster.util.Logging;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.WorkerTask;
import io.fluo.core.util.UtilWaitThread;
import org.apache.twill.api.AbstractTwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * Main run method of Fluo worker that can be called within
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

      FluoConfiguration config = new FluoConfiguration(new File(options.getFluoProps()));
      if (!config.hasRequiredWorkerProps()) {
        log.error("fluo.properties is missing required properties for worker");
        System.exit(-1);
      }
      
      Environment env = new Environment(config);

      log.info("Worker configuration:");
      env.getConfiguration().print();

      int numThreads = config.getWorkerThreads();

      ExecutorService tp = Executors.newFixedThreadPool(numThreads);
      for (int i = 0; i < numThreads; i++) {
        tp.submit(new WorkerTask(env, new AtomicBoolean(false)));
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
