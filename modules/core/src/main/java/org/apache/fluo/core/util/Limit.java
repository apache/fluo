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

package org.apache.fluo.core.util;

import com.google.common.base.Preconditions;

/**
 * This class is like a semaphore, but it allows acquiring more permits than exists which then
 * causes everything else to block. The idea is to block after the limit is exceeded.
 *
 */
public class Limit {
  int permits;
  int leased = 0;

  public Limit(int permits) {
    this.permits = permits;
  }

  public synchronized void acquire(int num) {
    Preconditions.checkArgument(num >= 0, "num < 0 : %s", num);
    while (leased >= permits) {
      try {
        wait();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    leased += num;
    if (leased < permits) {
      notify();
    }
  }


  public synchronized void release(int num) {
    Preconditions.checkArgument(num <= leased, "relasing more than leased %s > %s", num, leased);
    leased -= num;
    if (leased < permits) {
      notify();
    }
  }

  public synchronized int leased() {
    return leased;
  }
}
