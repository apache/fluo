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

    SimpleConfiguration expNoOrder = new SimpleConfiguration();
    expNoOrder.setProperty("set7", "value7");
    expNoOrder.setProperty("set4", "value4");
    expNoOrder.setProperty("set5", "value5");
    expNoOrder.setProperty("set1", "value2");
    expNoOrder.setProperty("set3", "value3");

    SimpleConfiguration diff = new SimpleConfiguration();
    diff.setProperty("set7", "value7");
    diff.setProperty("set4", "value11");
    diff.setProperty("set5", "value12");
    diff.setProperty("set1", "value13");
    diff.setProperty("set3", "value14");

    SimpleConfiguration sc4 = sc3.orElse(sc1).orElse(sc2);
    SimpleConfiguration noChange = new SimpleConfiguration(sc4);
    sc3.setProperty("set25", "value25");
    sc1.setProperty("set26", "value26");
    sc2.setProperty("set27", "value27");

    Assert.assertEquals(noChange, sc4);

    Assert.assertEquals(expected, msc);
    Assert.assertEquals(expNoOrder, msc);
    Assert.assertNotEquals(diff, msc);

    Assert.assertNotEquals(expected, sc1);
    Assert.assertNotEquals(expNoOrder, sc1);

    Assert.assertNotEquals(expected, sc2);
    Assert.assertNotEquals(expected, sc3);

    Assert.assertNotEquals(expNoOrder, sc2);
    Assert.assertNotEquals(expNoOrder, sc3);

    Assert.assertNotEquals(expNoOrder.toString(), msc.toString());
  }
}
