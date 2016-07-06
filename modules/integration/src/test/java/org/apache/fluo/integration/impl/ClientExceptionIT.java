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
import org.apache.fluo.integration.ITBaseMini;
import org.junit.Assert;
import org.junit.Test;

/**
 * Integration test to verify exceptions thrown by Fluo client
 */
public class ClientExceptionIT extends ITBaseMini {

  @Test
  public void testAlreadySetException() {

    // test transaction set & delete
    try (Transaction tx = client.newTransaction()) {
      tx.set(Bytes.of("row"), new Column("c1"), Bytes.of("val1"));
      tx.set(Bytes.of("row"), new Column("c1"), Bytes.of("val2"));
      Assert.fail("exception not thrown");
    } catch (AlreadySetException e) {
      // do nothing
    }

    try (Transaction tx = client.newTransaction()) {
      tx.set("row", new Column("c2"), "a");
      tx.set("row", new Column("c2"), "b");
      Assert.fail("exception not thrown");
    } catch (AlreadySetException e) {
      // do nothing
    }

    try (Transaction tx = client.newTransaction()) {
      tx.delete(Bytes.of("row"), new Column("c1"));
      tx.delete(Bytes.of("row"), new Column("c1"));
      Assert.fail("exception not thrown");
    } catch (AlreadySetException e) {
      // do nothing
    }
  }
}
