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
package io.fluo.api.mini;

import org.apache.commons.configuration.Configuration;

/**
 * A test and development instance of Fluo containing its own Oracle and Worker
 */
public interface MiniFluo extends AutoCloseable {

  /**
   * Retrieves the configuration required to connect to MiniFluo instance.
   */
  Configuration getClientConfiguration();

  /**
   * Waits for all observers to finish. This method is usually called by test code before
   * verification.
   */
  void waitForObservers();
}
