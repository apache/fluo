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

import java.util.Objects;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.atomic.AtomicValue;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.fluo.accumulo.util.LongUtil;
import org.apache.fluo.accumulo.util.ZookeeperPath;

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
