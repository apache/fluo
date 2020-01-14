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

package org.apache.fluo.command;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.fluo.api.config.FluoConfiguration;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConfigCommandTest {

  private ConfigCommand configCommand;

  @Before
  public void setUp() {
    configCommand = new ConfigCommand() {
      @Override
      public void execute() throws FluoCommandException {}
    };

    URL testConfig = getClass().getClassLoader().getResource("test-fluo-conn.properties");
    System.setProperty(CommandUtil.FLUO_CONN_PROPS, testConfig.getPath());
  }

  @Test
  public void testNullSplitter() {
    String testStr = "asdf, safggr = adfjc :";
    ConfigCommand.NullSplitter nullSplitter = new ConfigCommand.NullSplitter();

    List<String> nullSplitList = nullSplitter.split(testStr);

    assertEquals(1, nullSplitList.size());
    assertEquals(testStr, nullSplitList.get(0));
  }

  @Test
  public void testGetConfigWithOneOverriddenProp() {
    int newZookeeperTimeout = 100;
    List<String> overrideConfig = Collections.singletonList(
        FluoConfiguration.CONNECTION_ZOOKEEPER_TIMEOUT_PROP + "=" + newZookeeperTimeout);
    configCommand.setProperties(overrideConfig);

    FluoConfiguration fluoConfiguration = configCommand.getConfig();

    assertEquals(newZookeeperTimeout, fluoConfiguration.getZookeeperTimeout());
    assertEquals("app-name", fluoConfiguration.getApplicationName());
  }

  @Test
  public void testGetConfigWithTwoOverriddenProp() {
    int newZookeeperTimeout = 100;
    int loaderQueueSize = 256;
    List<String> overrideConfig = Arrays.asList(
        FluoConfiguration.CONNECTION_ZOOKEEPER_TIMEOUT_PROP + "=" + newZookeeperTimeout,
        FluoConfiguration.LOADER_QUEUE_SIZE_PROP + "=" + "256");
    configCommand.setProperties(overrideConfig);

    FluoConfiguration fluoConfiguration = configCommand.getConfig();

    assertEquals(newZookeeperTimeout, fluoConfiguration.getZookeeperTimeout());
    assertEquals(loaderQueueSize, fluoConfiguration.getLoaderQueueSize());
    assertEquals("app-name", fluoConfiguration.getApplicationName());
  }

  @Test(expected = FluoCommandException.class)
  public void testGetConfigInvalidOption() {
    configCommand.setProperties(Collections.singletonList("Invalid-Option"));

    configCommand.getConfig();
  }

  @Test(expected = FluoCommandException.class)
  public void testGetConfigMissingKey() {
    configCommand.setProperties(Collections.singletonList(" =value"));

    configCommand.getConfig();
  }

  @Test(expected = FluoCommandException.class)
  public void testGetConfigMissingValue() {
    configCommand.setProperties(Collections.singletonList("key= "));

    configCommand.getConfig();
  }
}
