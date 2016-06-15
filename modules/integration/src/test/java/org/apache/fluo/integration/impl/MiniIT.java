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

package org.apache.fluo.integration.impl;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.mini.MiniFluo;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests MiniFluo
 */
public class MiniIT {

  @Test
  public void testMini() throws Exception {
    File dataDir =
        new File(System.getProperty("user.dir") + "/target/" + MiniIT.class.getSimpleName());
    if (dataDir.exists()) {
      FileUtils.deleteDirectory(dataDir);
    }
    dataDir.mkdirs();
    try {
      FluoConfiguration config = new FluoConfiguration();
      config.setApplicationName("mini");
      config.setMiniDataDir(dataDir.getAbsolutePath());
      config.setMiniStartAccumulo(true);
      try (MiniFluo mini = FluoFactory.newMiniFluo(config)) {
        try (FluoClient client = FluoFactory.newClient(mini.getClientConfiguration())) {
          Assert.assertNotNull(client);
          Assert.assertNotNull(client.newLoaderExecutor());

          try (Transaction t = client.newTransaction()) {
            Assert.assertNotNull(t);
            t.set(Bytes.of("test"), new Column(Bytes.of("cf"), Bytes.of("cq")), Bytes.of("myval"));
            t.commit();
          }
          try (Snapshot s = client.newSnapshot()) {
            Assert.assertNotNull(s);
            Bytes v = s.get(Bytes.of("test"), new Column(Bytes.of("cf"), Bytes.of("cq")));
            Assert.assertEquals(Bytes.of("myval"), v);
          }
        }
      }
    } finally {
      FileUtils.deleteDirectory(dataDir);
    }
  }
}
