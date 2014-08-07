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
package io.fluo.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/** Provides cache of all Fluo transactors.
 * Used by clients to determine if transactor is running.
 */
public class TransactorCache implements Closeable {
  
  public enum TcStatus { OPEN, CLOSED };
  
  private Configuration config;
  private PathChildrenCache cache;
  private TcStatus status;
  private Cache<Long,AtomicLong> timeoutCache;
  
  private static Logger log = LoggerFactory.getLogger(TransactorCache.class);

  public TransactorCache(Configuration config) {

    timeoutCache = CacheBuilder.newBuilder().maximumSize(1 << 15).expireAfterAccess(TxInfoCache.CACHE_TIMEOUT_MIN, TimeUnit.MINUTES).concurrencyLevel(10)
        .build();

    this.config = config;
    cache = new PathChildrenCache(config.getSharedResources().getCurator(),
        TransactorID.getNodeRoot(config), true);
    try {
      cache.start(StartMode.BUILD_INITIAL_CACHE);
      status = TcStatus.OPEN;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private void logTimedoutTransactor(Long transactorId, long lockTs, Long startTime) {
    log.warn("Transactor ID {} was unresponsive for {} secs, marking as dead for lockTs <= {}", TransactorID.longToString(transactorId),
        (System.currentTimeMillis() - startTime) / 1000.0, lockTs);
  }

  public void addTimedoutTransactor(final Long transactorId, final long lockTs, final Long startTime) {

    try {
      AtomicLong cachedLockTs = timeoutCache.get(transactorId, new Callable<AtomicLong>() {
        @Override
        public AtomicLong call() throws Exception {
          logTimedoutTransactor(transactorId, lockTs, startTime);
          return new AtomicLong(lockTs);
        }
      });

      long currVal = cachedLockTs.get();

      while (lockTs > currVal) {
        if (cachedLockTs.compareAndSet(currVal, lockTs)) {
          logTimedoutTransactor(transactorId, lockTs, startTime);
        }

        // its possible another thread updates and the above compare and set failed, so the following will get us out of loop in this case... it will also get
        // us out of loop in case where compared and set succeeds
        currVal = cachedLockTs.get();
      }

    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean checkTimedout(Long transactorId, long lockTs) {
    AtomicLong timedoutLockTs = timeoutCache.getIfPresent(transactorId);
    return timedoutLockTs != null && lockTs <= timedoutLockTs.get();
  }

  public boolean checkExists(Long transactorId) {
    return cache.getCurrentData(TransactorID.getNodePath(config, transactorId)) != null;
  }
  
  public TcStatus getStatus() {
    return status;
  }

  @Override
  public void close() throws IOException {
    status = TcStatus.CLOSED;
    cache.close();
  }
}
