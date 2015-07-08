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

package io.fluo.api.client;

/**
 * Executes provided {@link Loader} objects to load data into Fluo. {@link LoaderExecutor#close()}
 * should be called when finished.
 */
public interface LoaderExecutor extends AutoCloseable {

  /**
   * Executes {@link Loader} implemented by users
   */
  void execute(Loader loader);

  /**
   * Closes resources
   */
  @Override
  void close();
}
