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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;

// created this class because batch writer blocks adding mutations while its flushing

public class SharedBatchWriter {

  private final BatchWriter bw;
  private ArrayBlockingQueue<MutationBatch> mutQueue = new ArrayBlockingQueue<>(1000);
  private MutationBatch end = new MutationBatch(new ArrayList<Mutation>());

  private AtomicLong asyncBatchesAdded = new AtomicLong(0);
  private long asyncBatchesProcessed = 0;

  private static class MutationBatch {

    private List<Mutation> mutations;
    private CountDownLatch cdl;
    private boolean isAsync = false;

    public MutationBatch(List<Mutation> mutations) {
      this.mutations = mutations;
      cdl = new CountDownLatch(1);
    }
  }

  private class FlushTask implements Runnable {

    @Override
    public void run() {
      boolean keepRunning = true;
      while (keepRunning) {
        try {

          ArrayList<MutationBatch> batches = new ArrayList<>();
          batches.add(mutQueue.take());
          mutQueue.drainTo(batches);

          for (MutationBatch mutationBatch : batches) {
            if (mutationBatch != end) {
              bw.addMutations(mutationBatch.mutations);
            }
          }

          bw.flush();

          int numAsync = 0;

          for (MutationBatch mutationBatch : batches) {
            if (mutationBatch == end) {
              keepRunning = false;
            }
            mutationBatch.cdl.countDown();

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

        } catch (Exception e) {
          // TODO error handling
          e.printStackTrace();
        }
      }

    }

  }

  public SharedBatchWriter(BatchWriter bw) {
    this.bw = bw;
    Thread thread = new Thread(new FlushTask());
    thread.setDaemon(true);
    thread.start();
  }

  public void writeMutation(Mutation m) {
    writeMutations(Collections.singletonList(m));
  }

  public void writeMutations(List<Mutation> ml) {

    if (ml.size() == 0) {
      return;
    }

    try {
      MutationBatch mb = new MutationBatch(ml);
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
      MutationBatch mb = new MutationBatch(ml);
      mb.isAsync = true;
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
