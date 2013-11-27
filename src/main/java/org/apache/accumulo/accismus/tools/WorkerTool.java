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
package org.apache.accumulo.accismus.tools;

import java.io.File;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.accismus.api.config.WorkerProperties;
import org.apache.accumulo.accismus.impl.Configuration;
import org.apache.accumulo.accismus.impl.Logging;
import org.apache.accumulo.accismus.impl.WorkerTask;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 
 */
public class WorkerTool extends Configured implements Tool {
  

  private static Logger log = Logger.getLogger(WorkerTool.class);



  public static void main(String[] args) throws Exception {
    ToolRunner.run(new WorkerTool(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Please supply a propeties files with the following defined : ");
      System.err.println();
      Configuration.getDefaultProperties().store(System.err, "Accismus properties");
      System.exit(-1);
    }

    Logging.init("worker");

    Configuration config = new Configuration(new File(args[0]));
    
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
  }
}
