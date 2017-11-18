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

import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.integration.ITBaseImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests TransactionImpl classes
 */
public class TransactionImplIT extends ITBaseImpl {

  @Test
  public void testgetsAsync() throws Exception {
    try (Transaction tx = client.newTransaction()) {
      tx.set("row1", new Column("col1"), "val1");
      tx.set("row2", new Column("col2"), "val2");
      tx.set("row3", new Column("col3"), "val3");

      tx.commit();
    }

    try (Transaction tx = client.newTransaction()) {
      Assert.assertEquals("val1", tx.getsAsync("row1", new Column("col1")).get());
      Assert.assertEquals("val2", tx.getsAsync("row2", new Column("col2"), "foo").get());
      Assert.assertEquals("val3", tx.getsAsync("row3", new Column("col3")).get());
      Assert.assertEquals("val4", tx.getsAsync("row4", new Column("col4"), "val4").get());
    }
  }

  @Test
  public void testgetAsync() throws Exception {
    Bytes row1 = Bytes.of("row1");
    Bytes row2 = Bytes.of("row2");
    Bytes row3 = Bytes.of("row3");
    Bytes row4 = Bytes.of("row4");

    Bytes val1 = Bytes.of("val1");
    Bytes val2 = Bytes.of("val2");
    Bytes val3 = Bytes.of("val3");
    Bytes val4 = Bytes.of("val4");

    try (Transaction tx = client.newTransaction()) {
      tx.set(row1, new Column("col1"), val1);
      tx.set(row2, new Column("col2"), val2);
      tx.set(row3, new Column("col3"), val3);

      tx.commit();
    }

    try (Transaction tx = client.newTransaction()) {
      Assert.assertEquals(val1, tx.getAsync(row1, new Column("col1")).get());
      Assert.assertEquals(val2, tx.getAsync(row2, new Column("col2"), Bytes.of("foo")).get());
      Assert.assertEquals(val3, tx.getAsync(row3, new Column("col3")).get());
      Assert.assertEquals(val4, tx.getAsync(row4, new Column("col4"), val4).get());
    }
  }


}
