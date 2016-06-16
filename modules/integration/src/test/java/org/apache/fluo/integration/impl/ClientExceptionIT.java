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
import org.apache.fluo.api.exceptions.AlreadySetException;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;
import org.apache.fluo.api.types.TypedTransaction;
import org.apache.fluo.integration.ITBaseMini;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration test to verify exceptions thrown by Fluo client
 */
public class ClientExceptionIT extends ITBaseMini {

  static TypeLayer tl = new TypeLayer(new StringEncoder());

  @Test
  public void testAlreadySetException() {

    // test transaction set & delete
    try (Transaction tx = client.newTransaction()) {
      tx.set(Bytes.of("row"), new Column("c1"), Bytes.of("val1"));
      tx.set(Bytes.of("row"), new Column("c1"), Bytes.of("val2"));
      Assert.fail("exception not thrown");
    } catch (AlreadySetException e) {
    }

    try (Transaction tx = client.newTransaction()) {
      tx.delete(Bytes.of("row"), new Column("c1"));
      tx.delete(Bytes.of("row"), new Column("c1"));
      Assert.fail("exception not thrown");
    } catch (AlreadySetException e) {
    }

    // test typed transactions
    // setting integer
    try (TypedTransaction tx = tl.wrap(client.newTransaction())) {
      tx.mutate().row("r1").col(new Column("c1")).set("a");
      tx.mutate().row("r1").col(new Column("c1")).set(6);
      Assert.fail("exception not thrown");
    } catch (AlreadySetException e) {
    }

    // test set setting empty twice
    try (TypedTransaction tx = tl.wrap(client.newTransaction())) {
      tx.mutate().row("r1").col(new Column("c1")).set();
      tx.mutate().row("r1").col(new Column("c1")).set();
      Assert.fail("exception not thrown");
    } catch (AlreadySetException e) {
    }

    // test boolean and same value
    try (TypedTransaction tx = tl.wrap(client.newTransaction())) {
      tx.mutate().row("r1").col(new Column("c1")).set(true);
      tx.mutate().row("r1").col(new Column("c1")).set(true);
      Assert.fail("exception not thrown");
    } catch (AlreadySetException e) {
    }

    // test string
    try (TypedTransaction tx = tl.wrap(client.newTransaction())) {
      tx.mutate().row("r1").col(new Column("c1")).set("a");
      tx.mutate().row("r1").col(new Column("c1")).set("b");
      Assert.fail("exception not thrown");
    } catch (AlreadySetException e) {
    }

    // test two deletes
    try (TypedTransaction tx = tl.wrap(client.newTransaction())) {
      tx.mutate().row("r1").col(new Column("c1")).delete();
      tx.mutate().row("r1").col(new Column("c1")).delete();
      Assert.fail("exception not thrown");
    } catch (AlreadySetException e) {
    }
  }
}
