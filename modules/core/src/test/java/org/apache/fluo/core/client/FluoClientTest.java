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

package org.apache.fluo.core.client;

import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class FluoClientTest {

  @Test
  public void testFailures() {

    // we are expecting errors in this test
    Level clientLevel = Logger.getLogger(FluoClientImpl.class).getLevel();
    Logger.getLogger(FluoClientImpl.class).setLevel(Level.FATAL);
    Level factoryLevel = Logger.getLogger(FluoFactory.class).getLevel();
    Logger.getLogger(FluoFactory.class).setLevel(Level.FATAL);

    FluoConfiguration config = new FluoConfiguration();
    try {
      FluoFactory.newClient(config);
      Assert.fail();
    } catch (FluoException e) {
    }

    try (FluoClientImpl impl = new FluoClientImpl(config)) {
      Assert.fail("FluoClientImpl was " + impl);
    } catch (IllegalArgumentException e) {
    }

    config.setApplicationName("test");
    config.setAccumuloUser("test");
    config.setAccumuloPassword("test");
    config.setAccumuloInstance("test");
    config.setZookeeperTimeout(5);

    try (FluoClientImpl impl = new FluoClientImpl(config)) {
      Assert.fail("FluoClientImpl was " + impl);
    } catch (IllegalStateException e) {
    }

    Logger.getLogger(FluoClientImpl.class).setLevel(clientLevel);
    Logger.getLogger(FluoFactory.class).setLevel(factoryLevel);
  }
}
