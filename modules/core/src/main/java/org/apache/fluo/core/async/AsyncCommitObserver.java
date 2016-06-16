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

/**
 * This interface is used for Asynchronous commit callback. Its expected that an asynchronous commit
 * will only call one of the methods below.
 *
 */

public interface AsyncCommitObserver {

  /**
   * Called when transactions is successfully committed asynchronously
   */
  void committed();

  /**
   * Called when async commit has an unexpected exception
   */
  void failed(Throwable t);

  /**
   * Called when async commit of a transaction is already acknowledged
   */
  void alreadyAcknowledged();

  /**
   * Called when async commit of a transaction fails because it overlapped with another transaction
   */
  void commitFailed();

}
