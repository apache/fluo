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

package org.apache.fluo.core.async;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.core.exceptions.AlreadyAcknowledgedException;

public class SyncCommitObserver implements AsyncCommitObserver {

  private CountDownLatch cdl = new CountDownLatch(1);
  private volatile boolean committed = false;
  private volatile boolean aacked = false;
  private volatile Exception error = null;
  private volatile String commitFailMsg = "";
  private AtomicBoolean isMethodCalled = new AtomicBoolean(false);
  private static final String METHOD_CALLED_VALIDATION_CHECK_MESSAGE =
      "One of the methods committed,failed,alreadyAcknowledged,commitFailed"
          + " was already called";

  @Override
  public void committed() {
    Preconditions.checkState(isMethodCalled.compareAndSet(false, true),
        METHOD_CALLED_VALIDATION_CHECK_MESSAGE);
    committed = Boolean.TRUE;
    cdl.countDown();
  }

  @Override
  public void failed(Throwable t) {
    Preconditions.checkState(isMethodCalled.compareAndSet(false, true),
        METHOD_CALLED_VALIDATION_CHECK_MESSAGE);
    error = (Exception) t;
    cdl.countDown();
  }

  @Override
  public void alreadyAcknowledged() {
    Preconditions.checkState(isMethodCalled.compareAndSet(false, true),
        METHOD_CALLED_VALIDATION_CHECK_MESSAGE);
    aacked = true;
    cdl.countDown();
  }

  @Override
  public void commitFailed(String msg) {
    Preconditions.checkState(isMethodCalled.compareAndSet(false, true),
        METHOD_CALLED_VALIDATION_CHECK_MESSAGE);
    committed = false;
    commitFailMsg = msg;
    cdl.countDown();
  }

  public void waitForCommit() {
    try {
      cdl.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    if (error != null) {
      throw new FluoException(error);
    } else if (aacked) {
      throw new AlreadyAcknowledgedException();
    } else if (!committed) {
      throw new CommitException(commitFailMsg);
    }
  }
}
