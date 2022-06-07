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

package org.apache.fluo.integration.client;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.core.util.AccumuloUtil;
import org.apache.fluo.integration.ITBaseImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FluoClientAuthorizationsIT extends ITBaseImpl {
  @Rule
  public Timeout globalTimeout = Timeout.seconds(getTestTimeout());

  Column ssn = new Column("", "ssn", "PRIVATE");
  Column name = new Column("", "name", "PUBLIC");
  Column id = new Column("", "id");
  FluoConfiguration conf;
  FluoClient client;

  @Before
  public void setupAuthorizations() throws Throwable {
    try (AccumuloClient accumulo = AccumuloUtil.getClient(config)) {
      accumulo.securityOperations().changeUserAuthorizations(config.getAccumuloUser(),
          new Authorizations("PRIVATE", "PUBLIC"));
    }
    this.conf = new FluoConfiguration(config);
    this.conf.setAccumuloAuthorizations("PRIVATE", "PUBLIC");
    this.client = FluoFactory.newClient(this.conf);

    writeSampleData();
  }

  /*
   * Kind of sloppy because we assert some basic write functionality already works. However, this
   * lets us re-use and organize tests that read this data better.
   */
  public void writeSampleData() {
    try (Transaction txn = client.newTransaction()) {
      txn.set("bill", ssn, "000-00-0001");
      txn.set("bill", name, "william");
      txn.set("bill", id, "1");
      txn.set("bob", ssn, "000-00-0002");
      txn.set("bob", name, "robert");
      txn.set("bob", id, "2");
      txn.commit();
    }
  }

  @After
  public void cleanupClient() throws Throwable {
    this.client.close();
  }

  @Test
  public void testBasicRead() {
    try (Snapshot snapshot = client.newSnapshot()) {
      Map<Column, String> bill = snapshot.gets("bill");
      assertTrue(bill.containsKey(name));
      assertTrue(bill.containsKey(ssn));
      assertTrue(bill.containsKey(id));
      Map<Column, String> bob = snapshot.gets("bill");
      assertTrue(bob.containsKey(name));
      assertTrue(bob.containsKey(ssn));
      assertTrue(bob.containsKey(id));
    }
  }

  @Test
  public void testPublicRead() {
    try (Snapshot snapshot =
        client.newSnapshot().useScanTimeAuthorizations(ImmutableList.of("PUBLIC"))) {
      Map<Column, String> bill = snapshot.gets("bill");
      assertTrue(bill.containsKey(name));
      assertTrue(bill.containsKey(id));
      assertEquals(2, bill.size());
    }
  }

  @Test
  public void testPrivateRead() {
    try (Snapshot snapshot =
        client.newSnapshot().useScanTimeAuthorizations(ImmutableList.of("PRIVATE"))) {
      Map<Column, String> bill = snapshot.gets("bill");
      assertTrue(bill.containsKey(ssn));
      assertTrue(bill.containsKey(id));
      assertEquals(2, bill.size());
    }
  }

  // had some initial uses where I checked for Authorizations.EMPTY instead of null
  // or empty set of auths, which caused the underlying scanner to scan at max
  // authorizations. I want this call to explicitly say "only read data that is
  // unlabeled"
  @Test
  public void testScanningWithNoAuths() {
    try (Snapshot snapshot =
        client.newSnapshot().useScanTimeAuthorizations(Collections.emptySet())) {
      Map<Column, String> bill = snapshot.gets("bill");
      assertFalse(bill.containsKey(name));
      assertFalse(bill.containsKey(ssn));
      assertTrue(bill.containsKey(id));
      Map<Column, String> bob = snapshot.gets("bob");
      assertFalse(bob.containsKey(name));
      assertFalse(bob.containsKey(ssn));
      assertTrue(bob.containsKey(id));
    }
  }

  @Test(expected = CommitException.class)
  public void testWriteUnreadable() {
    try (Transaction txn = client.newTransaction()) {
      txn.set("bill", new Column("", "unreadable", "UNREADABLE"), "value");
      txn.commit();
    }
  }
}
