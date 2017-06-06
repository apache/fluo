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

package org.apache.fluo.integration.client;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.core.client.FluoClientImpl;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class FluoClientIT extends ITBaseImpl {

  @Test
  public void testFailures() {

    // we are expecting errors in this test
    Level clientLevel = Logger.getLogger(FluoClientImpl.class).getLevel();
    Logger.getLogger(FluoClientImpl.class).setLevel(Level.FATAL);
    Level factoryLevel = Logger.getLogger(FluoFactory.class).getLevel();
    Logger.getLogger(FluoFactory.class).setLevel(Level.FATAL);

    FluoConfiguration fluoConfig = new FluoConfiguration();
    try {
      FluoFactory.newClient(fluoConfig);
      Assert.fail();
    } catch (FluoException e) {
    }

    try (FluoClientImpl impl = new FluoClientImpl(fluoConfig)) {
      Assert.fail("FluoClientImpl was " + impl);
    } catch (IllegalArgumentException e) {
    }

    try (FluoClient client = FluoFactory.newClient(config)) {
      client.newSnapshot();
    }

    try (FluoClientImpl client = new FluoClientImpl(config)) {
      client.newSnapshot();
    }

    Logger.getLogger(FluoClientImpl.class).setLevel(clientLevel);
    Logger.getLogger(FluoFactory.class).setLevel(factoryLevel);
  }
}
