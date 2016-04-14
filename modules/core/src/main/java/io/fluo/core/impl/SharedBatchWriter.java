/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.core.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;

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
    private MutationBatch afterFlushBatch;

    public MutationBatch(Collection<Mutation> mutations, boolean isAsync) {
      this.mutations = mutations;
      this.isAsync = isAsync;
      if (!isAsync) {
        this.cdl = new CountDownLatch(1);
      }
    }

    public MutationBatch(List<Mutation> mutations, List<Mutation> afterFlushBatch) {
      this.mutations = mutations;
      // both batches are async, but only want to increment count after 2nd batch is processed
      this.isAsync = false;
      this.afterFlushBatch = new MutationBatch(afterFlushBatch, true);
      this.cdl = null;
    }

    public void countDown() {
      if (cdl != null) {
        cdl.countDown();
      }
    }
  }

  private class FlushTask implements Runnable {

    @Override
    public void run() {
      boolean keepRunning = true;
      ArrayList<MutationBatch> batches = new ArrayList<>();
      ArrayList<MutationBatch> afterFlushBatches = new ArrayList<>();

      while (keepRunning || batches.size() > 0) {
        try {
          if (batches.size() == 0) {
            batches.add(mutQueue.take());
          }
          mutQueue.drainTo(batches);

          processBatches(batches);

          afterFlushBatches.clear();
          for (MutationBatch mutationBatch : batches) {
            if (mutationBatch.afterFlushBatch != null) {
              afterFlushBatches.add(mutationBatch.afterFlushBatch);
            }

            if (mutationBatch == end) {
              keepRunning = false;
            }
          }

          batches.clear();
          batches.addAll(afterFlushBatches);
        } catch (Exception e) {
          // TODO error handling
          e.printStackTrace();
        }
      }

    }

    private void processBatches(ArrayList<MutationBatch> batches) throws MutationsRejectedException {
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

  public SharedBatchWriter(BatchWriter bw) {
    this.bw = bw;
    this.mutQueue = new ArrayBlockingQueue<>(256);
    Thread thread = new Thread(new FlushTask());
    thread.setDaemon(true);
    thread.start();
  }

  public void writeMutation(Mutation m) {
    writeMutations(Collections.singletonList(m));
  }

  public void writeMutations(Collection<Mutation> ml) {

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

  public void close() {
    try {
      mutQueue.put(end);
      end.cdl.await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void writeMutationsAsync(List<Mutation> ml) {
    try {
      MutationBatch mb = new MutationBatch(ml, true);
      asyncBatchesAdded.incrementAndGet();
      mutQueue.put(mb);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write two mutation lists asynchronously, flushing the batch writer between writing the two.
   */
  public void writeMutationsAsync(List<Mutation> ml1, List<Mutation> ml2) {
    try {
      MutationBatch mb = new MutationBatch(ml1, ml2);
      asyncBatchesAdded.incrementAndGet();
      mutQueue.put(mb);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void writeMutationAsync(Mutation m) {
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
