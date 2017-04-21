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

package org.apache.fluo.core.observer;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.ObserverSpecification;
import org.apache.fluo.core.observer.v1.ObserverStoreV1;
import org.apache.fluo.core.observer.v2.ObserverStoreV2;
import org.junit.Assert;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class ObserverStoreTest {

  @Test
  public void testNewAndOldConfig() {
    ObserverStore ov1 = new ObserverStoreV1();
    ObserverStore ov2 = new ObserverStoreV2();

    FluoConfiguration config = new FluoConfiguration();
    Assert.assertFalse(ov1.handles(config));
    Assert.assertFalse(ov2.handles(config));

    config = new FluoConfiguration();
    config.setObserverProvider("TestProvider1");
    Assert.assertFalse(ov1.handles(config));
    Assert.assertTrue(ov2.handles(config));

    config = new FluoConfiguration();
    config.addObserver(new ObserverSpecification("TestProvider2"));
    Assert.assertTrue(ov1.handles(config));
    Assert.assertFalse(ov2.handles(config));
  }
}
