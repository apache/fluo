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
package org.fluo.api;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.fluo.api.config.LoaderExecutorProperties;
import org.fluo.impl.Configuration;
import org.fluo.impl.LoadTask;

/**
 * 
 * 
 */
public class LoaderExecutor {
  private ExecutorService executor;
  private Semaphore semaphore;
  
  private AtomicReference<Exception> exceptionRef = new AtomicReference<Exception>(null);
  private Configuration config;
  
  /**
   * 
   * @param props
   *          To programmatically initialize use {@link LoaderExecutorProperties}
   * @throws Exception
   */

  public LoaderExecutor(Properties props) throws Exception {
    this(props, Integer.parseInt(props.getProperty(LoaderExecutorProperties.NUM_THREADS_PROP, "10")), Integer.parseInt(props.getProperty(LoaderExecutorProperties.QUEUE_SIZE_PROP, "10")));
  }

  private LoaderExecutor(Properties connectionProps, int numThreads, int queueSize) throws Exception {
    if (numThreads == 0 && queueSize == 0) {
      this.config = new Configuration(connectionProps);
      return;
    }
    
    if (numThreads <= 0)
      throw new IllegalArgumentException("numThreads must be positivie OR numThreads and queueSize must both be 0");
    
    if (queueSize < 0)
      throw new IllegalArgumentException("queueSize must be non-negative OR numThreads and queueSize must both be 0");

    this.config = new Configuration(connectionProps);
    this.semaphore = new Semaphore(numThreads + queueSize);
    this.executor = Executors.newFixedThreadPool(numThreads);
  }
  
  // TODO exception handling model
  public void execute(Loader loader) {
    
    if (executor == null) {
      new LoadTask(loader, config).run();
    } else {
      if (exceptionRef.get() != null)
        throw new RuntimeException(exceptionRef.get());
      
      final Runnable lt = new LoadTask(loader, config);
      
      try {
        semaphore.acquire();
      } catch (InterruptedException e1) {
        throw new RuntimeException(e1);
      }
      
      Runnable eht = new Runnable() {
        
        @Override
        public void run() {
          try {
            lt.run();
          } catch (Exception e) {
            exceptionRef.compareAndSet(null, e);
          } finally {
            semaphore.release();
          }
        }
      };
      
      try {
        executor.execute(eht);
      } catch (RejectedExecutionException rje) {
        semaphore.release();
        throw rje;
      }
    }
  }

  
  public void shutdown() {
    if (executor != null) {
      executor.shutdown();
      while (!executor.isTerminated()) {
        try {
          executor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
    
    config.getSharedResources().close();

    if (exceptionRef.get() != null)
      throw new RuntimeException(exceptionRef.get());
  }
  
}
