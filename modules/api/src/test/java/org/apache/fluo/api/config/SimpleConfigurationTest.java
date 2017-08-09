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
package org.apache.fluo.api.config;

import org.junit.Assert;
import org.junit.Test;

/**
 * @since 1.2.0
 */
public class SimpleConfigurationTest {

  @Test
  public void testMerge() {
    SimpleConfiguration empty = new SimpleConfiguration();
    SimpleConfiguration sc1 = new SimpleConfiguration();
    SimpleConfiguration sc2 = new SimpleConfiguration();
    SimpleConfiguration sc3 = new SimpleConfiguration();
    SimpleConfiguration testEmpty = sc1.orElse(sc2).orElse(sc3);

    Assert.assertEquals(empty, testEmpty);

    sc1.setProperty("set1", "value1");
    sc1.setProperty("set1", "value2");
    sc1.setProperty("set3", "value3");
    sc2.setProperty("set4", "value4");
    sc2.setProperty("set5", "value5");
    sc2.setProperty("set1", "value6"); // wont include as sc1 already has set1
    sc3.setProperty("set7", "value7");
    sc3.setProperty("set5", "value8"); // wont include as sc2 already has set5

    SimpleConfiguration msc = sc1.orElse(sc2).orElse(sc3);

    SimpleConfiguration expected = new SimpleConfiguration();
    expected.setProperty("set1", "value2");
    expected.setProperty("set3", "value3");
    expected.setProperty("set4", "value4");
    expected.setProperty("set5", "value5");
    expected.setProperty("set7", "value7");

    Assert.assertEquals(expected, msc);
  }
}
