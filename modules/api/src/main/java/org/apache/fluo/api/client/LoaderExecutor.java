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
   */
  void execute(Loader loader);

  /**
   * Same as {@link #execute(Loader)}, but allows specifing an identity. The identity is used in
   * metrics and trace logging. When an identity is not supplied, the class name is used. In the
   * case of lambdas the class name may not be the same in different processes.
   * 
   * @since 1.1.0
   */
  void execute(String identity, Loader loader);

  /**
   * Waits for all queued and running Loader task to complete, then cleans up resources.
   */
  @Override
  void close();
}
