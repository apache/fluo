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

import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;

/** Provides cache of all Fluo transactors.
 * Used by clients to determine if transactor is running.
 */
public class TransactorCache implements Closeable {
  
  public enum TcStatus { OPEN, CLOSED };
  
  private Configuration config;
  private PathChildrenCache cache;
  private TcStatus status;
  
  public TransactorCache(Configuration config) {
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
