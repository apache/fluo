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

package org.apache.fluo.api.client;

import java.util.concurrent.CompletableFuture;

/**
 * Executes provided {@link Loader} objects to load data into Fluo. {@link LoaderExecutor#close()}
 * should be called when finished.
 *
 * @since 1.0.0
 */
public interface LoaderExecutor extends AutoCloseable {

  /**
   * Queues {@link Loader} task implemented by users for execution. The load Task may not have
   * completed when the method returns. If the queue is full, this method will block.
   *
   * <p>
   * If a previous execution of loader has thrown an exception, then this call may throw an
   * exception. To avoid this odd behavior use {@link #submit(Loader)} instead which relays
   * exceptions through the returned future.
   */
  void execute(Loader loader);

  /**
   * Same as {@link #execute(Loader)}, but allows specifying an identity. The identity is used in
   * metrics and trace logging. When an identity is not supplied, the class name is used. In the
   * case of lambdas the class name may not be the same in different processes.
   *
   * @since 1.1.0
   */
  void execute(String identity, Loader loader);


  /**
   * Same as {@link #execute(Loader)} except it returns a future that completes upon successful
   * commit and if an exception is thrown in the loader, it will be relayed through the future. The
   * result of the future is the Loader that was successfully executed. If storing any information
   * in the loader object, keep in mind that loaders may execute multiple times in the case of
   * commit collisions. If a loader executes multiple times, it may see different data on subsequent
   * executions.
   *
   * @since 2.0.0
   */
  <T extends Loader> CompletableFuture<T> submit(T loader);


  /**
   * Same behavior as {@link #submit(Loader)}.
   *
   * @param identity see {@link #execute(String, Loader)} for a description of this parameter
   * @since 2.0.0
   */
  <T extends Loader> CompletableFuture<T> submit(String identity, T loader);

  /**
   * Waits for all queued and running Loader task to complete, then cleans up resources.
   *
   * <p>
   * If a loader executed via {@link #execute(Loader)} or {@link #execute(String, Loader)} threw an
   * exception then this method will throw an exception. Exceptions thrown by loaders executed using
   * {@link #submit(Loader)} or {@link #submit(String, Loader)} will never cause this method to
   * throw an exception.
   */
  @Override
  void close();
}
