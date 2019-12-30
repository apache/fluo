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
import java.util.Collections;
import java.util.List;

import org.apache.fluo.api.config.FluoConfiguration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AppCommandTest {

  @Test
  public void testGetConfig() {
    AppCommand appCommand = new AppCommand() {
      @Override
      public void execute() throws FluoCommandException {}
    };

    URL testConfig = getClass().getClassLoader().getResource("test-fluo-conn.properties");
    System.setProperty(CommandUtil.FLUO_CONN_PROPS, testConfig.getPath());

    String newAppName = "new-app-name";
    int newZookeeperTimeout = 100;
    List<String> overrideConfig = Collections.singletonList(
        FluoConfiguration.CONNECTION_ZOOKEEPER_TIMEOUT_PROP + "=" + newZookeeperTimeout);
    appCommand.setApplicationName(newAppName);
    appCommand.setProperties(overrideConfig);

    FluoConfiguration fluoConfiguration = appCommand.getConfig();

    assertEquals(newAppName, fluoConfiguration.getApplicationName());
    assertEquals(newZookeeperTimeout, fluoConfiguration.getZookeeperTimeout());
  }
}
