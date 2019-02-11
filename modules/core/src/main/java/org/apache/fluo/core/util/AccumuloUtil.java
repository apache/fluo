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

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.fluo.api.config.FluoConfiguration;

/**
 * Utilities for interacting with Accumulo
 */
public class AccumuloUtil {


  /**
   * Creates Accumulo connector given FluoConfiguration
   */
  public static AccumuloClient getClient(FluoConfiguration config) {
    return Accumulo.newClient().to(config.getAccumuloInstance(), config.getAccumuloZookeepers())
        .as(config.getAccumuloUser(), config.getAccumuloPassword()).build();
  }
}
