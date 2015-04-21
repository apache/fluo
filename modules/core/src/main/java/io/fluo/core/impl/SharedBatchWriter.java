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

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;

// created this class because batch writer blocks adding mutations while its flushing

public class SharedBatchWriter {

  private final BatchWriter bw;
  private ArrayBlockingQueue<MutationBatch> mQueue = new ArrayBlockingQueue<>(1000);
  private MutationBatch end = new MutationBatch(new ArrayList<Mutation>());

  private static class MutationBatch {

    private List<Mutation> mutations;
    private CountDownLatch cdl;

    public MutationBatch(Mutation m) {
      mutations = Collections.singletonList(m);
      cdl = new CountDownLatch(1);
    }

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
          batches.add(mQueue.take());
          mQueue.drainTo(batches);

          for (MutationBatch mutationBatch : batches) {
            if (mutationBatch != end)
              bw.addMutations(mutationBatch.mutations);
          }

          bw.flush();

          for (MutationBatch mutationBatch : batches) {
            if (mutationBatch == end)
              keepRunning = false;
            mutationBatch.cdl.countDown();
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
    try {
      MutationBatch mb = new MutationBatch(m);
      mQueue.put(mb);
      mb.cdl.await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void writeMutations(List<Mutation> ml) {

    if (ml.size() == 0)
      return;

    try {
      MutationBatch mb = new MutationBatch(ml);
      mQueue.put(mb);
      mb.cdl.await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    try {
      mQueue.put(end);
      end.cdl.await();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void writeMutationAsync(Mutation m) {
    try {
      MutationBatch mb = new MutationBatch(m);
      mQueue.put(mb);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
