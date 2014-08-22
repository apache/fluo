/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.core.impl;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import io.fluo.accumulo.util.LongUtil;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A transactor performs transactions in Fluo.
 * This class gives a transactor a unique ID and registers the transactor 
 * in Zookeeper
 */
public class TransactorID implements Closeable {
  
  public enum TrStatus { OPEN, CLOSED };
  
  private static Logger log = LoggerFactory.getLogger(TransactorID.class);
  private Environment env;
  private PersistentEphemeralNode node;
  private Long id;
  private TrStatus status;
  
  public TransactorID(Environment env) {
    this.env = env;
    try {
      id = createID();
      createEphemeralNode();
      status = TrStatus.OPEN;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  public Long getLongID() {
    if (status == TrStatus.CLOSED) {
      throw new IllegalStateException("TransactorID is closed!");
    }
    return id;
  }
  
  public String getStringID() {
    return LongUtil.longToString(getLongID());
  }
  
  public TrStatus getStatus() {
    return status;
  }
  
  @Override
  public void close() throws IOException {
    status = TrStatus.CLOSED;
    node.close();
  }
  
  @VisibleForTesting
  public String getNodePath() {
    return getNodePath(env, id);
  }
  
  public static String getNodePath(Environment env, Long transactorId) {
    return getNodeRoot(env) + "/" + LongUtil.longToString(transactorId);
  }
  
  public static String getNodeRoot(Environment env) {
    return env.getZookeeperRoot() + ZookeeperConstants.TRANSACTOR_NODES;
  }
  
  private Long createID() throws Exception {
    DistributedAtomicLong counter = new DistributedAtomicLong(
        env.getSharedResources().getCurator(), 
        env.getZookeeperRoot() + ZookeeperConstants.TRANSACTOR_COUNT,
        new ExponentialBackoffRetry(1000, 10));
    AtomicValue<Long> nextId = counter.increment();
    while (nextId.succeeded() == false) {
      nextId = counter.increment();
    }
    return nextId.postValue();
  }
  
  private void createEphemeralNode() throws InterruptedException {
    node = new PersistentEphemeralNode(env.getSharedResources().getCurator(), 
        Mode.EPHEMERAL, getNodePath(), getStringID().getBytes());
    node.start();
    int waitTime = 0;
    while (node.waitForInitialCreate(1, TimeUnit.SECONDS) == false) {
      waitTime += 1;
      log.info("Waited "+waitTime+" sec for ephmeral node to be created");
      if (waitTime > 10) {
        throw new RuntimeException("Failed to create transactor ephemeral node");
      }
    }
  }
}
