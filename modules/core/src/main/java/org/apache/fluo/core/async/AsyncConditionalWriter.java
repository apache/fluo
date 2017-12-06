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

package org.apache.fluo.core.async;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.apache.fluo.core.util.FluoExecutors;
import org.apache.fluo.core.util.Limit;

public class AsyncConditionalWriter {

  private final ConditionalWriter cw;
  private final ExecutorService es;
  private final Limit semaphore;


  public AsyncConditionalWriter(Environment env, ConditionalWriter cw) {
    this.cw = cw;
    int numThreads = env.getConfiguration().getInt(FluoConfigurationImpl.ASYNC_CW_THREADS,
        FluoConfigurationImpl.ASYNC_CW_THREADS_DEFAULT);
    int permits = env.getConfiguration().getInt(FluoConfigurationImpl.ASYNC_CW_LIMIT,
        FluoConfigurationImpl.ASYNC_CW_LIMIT_DEFAULT);
    this.es = FluoExecutors.newFixedThreadPool(numThreads, "asyncCw");
    // the conditional writer currently has not memory limits... give it too much and it blows out
    // memory.. need to fix this in conditional writer
    // for now this needs to be memory based
    this.semaphore = new Limit(permits);
  }

  public CompletableFuture<Iterator<Result>> apply(Collection<ConditionalMutation> input) {
    if (input.size() == 0) {
      return CompletableFuture.completedFuture(Collections.<Result>emptyList().iterator());
    }

    semaphore.acquire(input.size());
    Iterator<Result> iter = cw.write(input.iterator());
    return CompletableFuture.supplyAsync(() -> {
      try {
        Builder<Result> imlb = ImmutableList.builder();
        while (iter.hasNext()) {
          Result result = iter.next();
          imlb.add(result);
        }
        return imlb.build().iterator();
      } finally {
        semaphore.release(input.size());
      }
    }, es);
  }

  public void close() {
    es.shutdownNow();
    try {
      es.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
