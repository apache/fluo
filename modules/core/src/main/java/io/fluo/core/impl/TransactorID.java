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

import java.util.Objects;

import io.fluo.accumulo.util.LongUtil;
import io.fluo.accumulo.util.ZookeeperPath;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Identifier allocated from Zookeeper that uniquely identifies a transactor. A transactor is any
 * client the performs transactions in Fluo
 */
public class TransactorID {

  private final Long id;

  public TransactorID(CuratorFramework curator) {
    Objects.requireNonNull(curator);
    id = createID(curator);
  }

  public TransactorID(Environment env) {
    this(env.getSharedResources().getCurator());
  }

  /**
   * Retrieves Long representation of identifier
   */
  public Long getLongID() {
    return id;
  }

  /**
   * Outputs identifier as String (using max radix)
   */
  @Override
  public String toString() {
    return LongUtil.toMaxRadixString(getLongID());
  }

  private static Long createID(CuratorFramework curator) {
    try {
      DistributedAtomicLong counter =
          new DistributedAtomicLong(curator, ZookeeperPath.TRANSACTOR_COUNT,
              new ExponentialBackoffRetry(1000, 10));
      AtomicValue<Long> nextId = counter.increment();
      while (nextId.succeeded() == false) {
        nextId = counter.increment();
      }
      return nextId.postValue();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
