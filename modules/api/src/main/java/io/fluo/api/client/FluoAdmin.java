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
package io.fluo.api.client;

import io.fluo.api.config.FluoConfiguration;

/**
 * Provides methods for initializing and administering a Fluo instance.
 */
public interface FluoAdmin {
  
  /**
   * Exception that is thrown if Fluo instance was already initialized. An instance is already initialized if the root directory set by the property
   * io.fluo.client.zookeeper.connect exists in Zookeeper. If this directory can be reinitialized and overwritten set io.fluo.client.zookeeper.clear to true
   */
  public static class AlreadyInitializedException extends Exception {
    private static final long serialVersionUID = 1L;

    public AlreadyInitializedException(Exception e) {
      super(e);
    }
  }

  /**
   * Initializes Fluo instance and stores shared configuration in Zookeeper. Shared configuration consists of properties with
   * {@value io.fluo.api.config.FluoConfiguration#APP_PREFIX}, {@value io.fluo.api.config.FluoConfiguration#OBSERVER_PREFIX} and
   * {@value io.fluo.api.config.FluoConfiguration#TRANSACTION_PREFIX} prefixes. Throws {@link AlreadyInitializedException} if Fluo instance was already
   * initialized in zookeeper. If you want initialize zookeeper again, set {@value FluoConfiguration#ADMIN_ALLOW_REINITIALIZE_PROP} to true
   */
  public void initialize() throws AlreadyInitializedException;

  /**
   * Updates shared configuration in Zookeeper. Shared configuration consists of properties with {@value io.fluo.api.config.FluoConfiguration#APP_PREFIX},
   * {@value io.fluo.api.config.FluoConfiguration#OBSERVER_PREFIX} and {@value io.fluo.api.config.FluoConfiguration#TRANSACTION_PREFIX} prefixes. This
   * method is called if a user has previously called {@link #initialize()} but wants changes to shared configuration updated in Zookeeper.
   * 
   * <p>
   * During this method Observers are reinitialized using configuration passed to FluoAdmin and not existing shared configuration stored in zookeeper. So make
   * sure all config needed by observers is present.
   */
  public void updateSharedConfig();
}
