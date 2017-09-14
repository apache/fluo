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

package org.apache.fluo.core.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

public class FluoExecutors {
  public static ExecutorService newFixedThreadPool(int numThreads, String name) {
    return newFixedThreadPool(numThreads, new LinkedBlockingQueue<Runnable>(), name);
  }

  public static ThreadPoolExecutor newFixedThreadPool(int numThreads, BlockingQueue<Runnable> queue,
      String name) {
    ThreadPoolExecutor tpe = new ThreadPoolExecutor(numThreads, numThreads, 0L,
        TimeUnit.MILLISECONDS, queue, new FluoThreadFactory(name)) {
      @Override
      protected void afterExecute(Runnable r, Throwable t) {
        if (t != null) {
          if (t instanceof Exception) {
            LoggerFactory.getLogger(FluoExecutors.class).warn("Thread pool saw uncaught Exception",
                t);
          } else {
            // this is likely an Error. Things may be in a really bad state, so just print it
            // instead of logging.
            System.err.println("Threadpool saw uncaught Throwable");
            t.printStackTrace();
          }
        }
      }
    };
    return tpe;
  }
}
