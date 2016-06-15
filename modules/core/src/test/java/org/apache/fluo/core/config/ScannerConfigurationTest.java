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

package org.apache.fluo.core.config;

import org.apache.fluo.api.config.ScannerConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for ScannerConfiguration class
 */
public class ScannerConfigurationTest {

  @Test
  public void testSetGet() {

    ScannerConfiguration config = new ScannerConfiguration();
    Assert.assertEquals(new Span(), config.getSpan());
    Assert.assertEquals(0, config.getColumns().size());

    config = new ScannerConfiguration();
    config.setSpan(Span.exact("row1"));
    Assert.assertEquals(Span.exact("row1"), config.getSpan());
    Assert.assertEquals(0, config.getColumns().size());

    config = new ScannerConfiguration();
    config.fetchColumnFamily(Bytes.of("cf1"));
    Assert.assertEquals(1, config.getColumns().size());
    Assert.assertEquals(new Column("cf1"), config.getColumns().iterator().next());

    config = new ScannerConfiguration();
    config.fetchColumn(Bytes.of("cf2"), Bytes.of("cq2"));
    Assert.assertEquals(1, config.getColumns().size());
    Assert.assertEquals(new Column("cf2", "cq2"), config.getColumns().iterator().next());

    config = new ScannerConfiguration();
    config.fetchColumnFamily(Bytes.of("a"));
    config.fetchColumnFamily(Bytes.of("b"));
    config.fetchColumnFamily(Bytes.of("a"));
    Assert.assertEquals(2, config.getColumns().size());

    config.clearColumns();
    Assert.assertEquals(0, config.getColumns().size());
  }

  @Test
  public void testNullSet() {

    ScannerConfiguration config = new ScannerConfiguration();

    try {
      config.setSpan(null);
      Assert.fail();
    } catch (NullPointerException e) {
    }

    try {
      config.fetchColumnFamily(null);
      Assert.fail();
    } catch (NullPointerException e) {
    }

    try {
      config.fetchColumn(null, Bytes.of("qual"));
      Assert.fail();
    } catch (NullPointerException e) {
    }

    try {
      config.fetchColumn(Bytes.of("fam"), null);
      Assert.fail();
    } catch (NullPointerException e) {
    }

  }
}
