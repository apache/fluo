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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.fluo.api.config.FluoConfiguration;

/**
 * Utilities for interacting with Accumulo
 */
public class AccumuloUtil {

  /**
   * Creates Accumulo instance given FluoConfiguration
   */
  public static Instance getInstance(FluoConfiguration config) {
    ClientConfiguration clientConfig =
        new ClientConfiguration().withInstance(config.getAccumuloInstance())
            .withZkHosts(config.getAccumuloZookeepers())
            .withZkTimeout(config.getZookeeperTimeout() / 1000);
    return new ZooKeeperInstance(clientConfig);
  }

  /**
   * Creates Accumulo connector given FluoConfiguration
   */
  public static Connector getConnector(FluoConfiguration config) {
    try {
      return getInstance(config).getConnector(config.getAccumuloUser(),
          new PasswordToken(config.getAccumuloPassword()));
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new IllegalStateException(e);
    }
  }
}
