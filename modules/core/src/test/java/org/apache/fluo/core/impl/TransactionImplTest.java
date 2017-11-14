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
package org.apache.fluo.core.impl;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.core.client.FluoClientImpl;
import org.junit.Assert;
import org.junit.Test;

public class TransactionImplTest {
  @Test
  public void testBasic() {
    //   FluoConfiguration fluoConfig = new FluoConfiguration();
    //   // fluoConfig.setApplicationName(config.getApplicationName());
    //   // fluoConfig.setInstanceZookeepers(config.getInstanceZookeepers());
    //   Environment env = new Environment(fluoConfig);
    //   TransactionBase tx = new TestTransaction(env);

    //   /*try (FluoClient client = FluoFactory.newClient(fluoConfig);
    //       Transaction tx = client.newTransaction()) {*/

    //     tx.set("row1", new Column("col1"), "val1");
    //     tx.set("row2", new Column("col2"), "val2");
    //     tx.set("row3", new Column("col3"), "val3");

    //     tx.commit();

    //     try {
    //       Assert.assertEquals("val1", tx.getsAsync("row1", new Column("col1")).get());
    //       Assert.assertEquals("val2", tx.getsAsync("row2", new Column("col2")).get());
    //       Assert.assertEquals("val3", tx.getsAsync("row3", new Column("col3")).get());

    //       Assert.assertEquals("val4", tx.getsAsync("row4", new Column("col4"), "val4").get());
    //     } catch (Exception e) {
    //       System.out.println(e.toString());
    //     }
    //   /*}*/
  }
}
