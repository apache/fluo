package org.fluo.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.data.Mutation;

//created this class because batch writer blocks adding mutations while its flushing

public class SharedBatchWriter {

  private BatchWriter bw;
  private ArrayBlockingQueue<MutationBatch> mQueue = new ArrayBlockingQueue<MutationBatch>(1000);
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

          ArrayList<MutationBatch> batches = new ArrayList<MutationBatch>();
          batches.add(mQueue.take());
          mQueue.drainTo(batches);

          for (MutationBatch mutationBatch : batches) {
            if(mutationBatch != end)
              bw.addMutations(mutationBatch.mutations);
          }

          // TODO the goal w/ this is that mutations written by separate transactions can be batched together.... however there is a drawback to this approach,
          // after a transactions mutations are written it may have to wait for writes to other tservers... ideally we could batch writes to tserver from
          // different transactions AND not have transactions wait on writes to unrelated tservers.... this is how the conditional writer works....
          bw.flush();

          for (MutationBatch mutationBatch : batches) {
            if(mutationBatch == end)
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
    // TODO check if closed
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
    //TODO check if closed
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

