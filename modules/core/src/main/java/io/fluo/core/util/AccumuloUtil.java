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
package io.fluo.core.util;

import io.fluo.api.config.FluoConfiguration;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

/**
 * Utilities for interacting with Accumulo
 */
public class AccumuloUtil {
  
  /**
   * Creates Accumulo instance given FluoConfiguration
   */
  public static Instance getInstance(FluoConfiguration config) {
    return new ZooKeeperInstance(config.getAccumuloInstance(), config.getAccumuloZookeepers());
  }

  /**
   * Creates Accumulo connector given FluoConfiguration 
   */
  public static Connector getConnector(FluoConfiguration config) {
    try {
      return getInstance(config).getConnector(config.getAccumuloUser(), new PasswordToken(config.getAccumuloPassword()));
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IllegalStateException(e);
    }
  }
}