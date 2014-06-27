package org.fluo.impl;

public class TxStats {
  private long lockWaitTime = 0;
  private long entriesReturned = 0;
  private long entriesSet = 0;
  private long startTime = 0;
  private long finishTime = 0;
  private long collisions = 0;
  // number of entries recovered from other transactions
  private long recovered = 0;

  TxStats() {
    this.startTime = System.currentTimeMillis();
  }

  public long getLockWaitTime() {
    return lockWaitTime;
  }

  public long getEntriesReturned() {
    return entriesReturned;
  }

  public long getEntriesSet() {
    return entriesSet;
  }

  public long getTime() {
    return finishTime - startTime;
  }

  public long getCollisions() {
    return collisions;
  }

  public long getRecovered() {
    return recovered;
  }

  void incrementLockWaitTime(long l) {
    lockWaitTime += l;
  }

  void incrementEntriesReturned(long l) {
    entriesReturned += l;
  }

  void incrementEntriesSet(long l) {
    entriesSet += l;
  }

  void incrementCollisions(long c) {
    collisions += c;
  }


  void setFinishTime(long t) {
    finishTime = t;
  }
}
