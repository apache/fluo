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

package org.apache.fluo.core.impl;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.fluo.core.util.FluoThreadFactory;

// created this class because batch writer blocks adding mutations while its flushing

public class SharedBatchWriter {

  private final BatchWriter bw;
  private ArrayBlockingQueue<MutationBatch> mutQueue;
  private MutationBatch end = new MutationBatch(new ArrayList<Mutation>(), false);

  private AtomicLong asyncBatchesAdded = new AtomicLong(0);
  private long asyncBatchesProcessed = 0;

  private static class MutationBatch {

    private Collection<Mutation> mutations;
    private CountDownLatch cdl;
    private boolean isAsync = false;
    private ListenableFutureTask<Void> lf;

    public MutationBatch(Collection<Mutation> mutations, boolean isAsync) {
      this.mutations = mutations;
      this.isAsync = isAsync;
      if (!isAsync) {
        this.cdl = new CountDownLatch(1);
      }
    }

    public MutationBatch(Collection<Mutation> mutations, ListenableFutureTask<Void> lf) {
      this.mutations = mutations;
      this.lf = lf;
      this.cdl = null;
      this.isAsync = false;
    }

    public void countDown() {
      if (cdl != null) {
        cdl.countDown();
      }

      if (lf != null) {
        lf.run();
      }
    }
  }

  private class FlushTask implements Runnable {

    @Override
    public void run() {
      boolean keepRunning = true;
      ArrayList<MutationBatch> batches = new ArrayList<>();

      while (keepRunning || batches.size() > 0) {
        try {
          if (batches.size() == 0) {
            batches.add(mutQueue.take());
          }
          mutQueue.drainTo(batches);

          processBatches(batches);

          for (MutationBatch mutationBatch : batches) {
            if (mutationBatch == end) {
              keepRunning = false;
            }
          }

          batches.clear();
        } catch (Exception e) {
          // TODO error handling
          e.printStackTrace();
        }
      }

    }

    private void processBatches(ArrayList<MutationBatch> batches)
        throws MutationsRejectedException {
      for (MutationBatch mutationBatch : batches) {
        if (mutationBatch != end) {
          bw.addMutations(mutationBatch.mutations);
        }
      }

      bw.flush();

      int numAsync = 0;

      for (MutationBatch mutationBatch : batches) {
        mutationBatch.countDown();

        if (mutationBatch.isAsync) {
          numAsync++;
        }
      }

      if (numAsync > 0) {
        synchronized (SharedBatchWriter.this) {
          asyncBatchesProcessed += numAsync;
          SharedBatchWriter.this.notifyAll();
        }
      }
    }
  }

  SharedBatchWriter(BatchWriter bw) {
    this.bw = bw;
    this.mutQueue = new ArrayBlockingQueue<>(100000);
    Thread thread = new FluoThreadFactory("sharedBW").newThread(new FlushTask());
    thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        System.err.println("Uncaught exception in shared batch writer");
        e.printStackTrace();
      }
    });
    thread.setDaemon(true);
    thread.start();
  }

  void writeMutation(Mutation m) {
    writeMutations(Collections.singletonList(m));
  }

  void writeMutations(Collection<Mutation> ml) {

    if (ml.size() == 0) {
      return;
    }

    try {
      MutationBatch mb = new MutationBatch(ml, false);
      mutQueue.put(mb);
      mb.cdl.await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final Runnable DO_NOTHING = new Runnable() {
    @Override
    public void run() {}
  };

  ListenableFuture<Void> writeMutationsAsyncFuture(Collection<Mutation> ml) {
    if (ml.size() == 0) {
      return Futures.immediateFuture(null);
    }

    ListenableFutureTask<Void> lf = ListenableFutureTask.create(DO_NOTHING, null);
    try {
      MutationBatch mb = new MutationBatch(ml, lf);
      mutQueue.put(mb);
      return lf;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  ListenableFuture<Void> writeMutationsAsyncFuture(Mutation m) {
    return writeMutationsAsyncFuture(Collections.singleton(m));
  }

  void close() {
    try {
      mutQueue.put(end);
      end.cdl.await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void writeMutationsAsync(List<Mutation> ml) {
    try {
      MutationBatch mb = new MutationBatch(ml, true);
      asyncBatchesAdded.incrementAndGet();
      mutQueue.put(mb);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  void writeMutationAsync(Mutation m) {
    writeMutationsAsync(Collections.singletonList(m));
  }

  /**
   * waits for all async mutations that were added before this was called to be flushed. Does not
   * wait for async mutations added after call.
   */
  public void waitForAsyncFlush() {
    long numAdded = asyncBatchesAdded.get();

    synchronized (this) {
      while (numAdded > asyncBatchesProcessed) {
        try {
          wait();
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
