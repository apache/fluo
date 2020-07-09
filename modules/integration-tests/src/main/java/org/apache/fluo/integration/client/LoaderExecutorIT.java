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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.AlreadySetException;
import org.apache.fluo.integration.ITBaseMini;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class LoaderExecutorIT extends ITBaseMini {

  public static class BadLoader implements Loader {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(getTestTimeout());

    @Override
    public void load(TransactionBase tx, Context context) throws Exception {
      tx.set("r", new Column("f", "q"), "v");
      // setting same thing should cause exception
      tx.set("r", new Column("f", "q"), "v2");
    }

  }

  @Test
  public void testLoaderFailure() {
    LoaderExecutor le = client.newLoaderExecutor();
    le.execute(new BadLoader());

    try {
      le.close();
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertEquals(AlreadySetException.class, e.getCause().getClass());
    }
  }

  @Test
  public void testSubmit() throws Exception {

    LoaderExecutor le = client.newLoaderExecutor();

    List<CompletableFuture<Void>> futures = new ArrayList<>();

    futures.add(le.submit("test", (tx, ctx) -> {
      tx.set("1234", new Column("last", "date"), "20060101");
    }));

    futures.add(le.submit("test", (tx, ctx) -> {
      tx.set("6789", new Column("last", "date"), "20050101");
    }));

    futures.add(le.submit("test", (tx, ctx) -> {
      tx.set("0abc", new Column("last", "date"), "20070101");
    }));

    futures.add(le.submit("test", (tx, ctx) -> {
      tx.set("ef01", new Column("last", "date"), "20040101");
      // setting same thing should cause exception
      tx.set("ef01", new Column("last", "date"), "20040101");
    }));


    // wait for transaction to commit
    futures.get(0).get();
    try (Snapshot snap = client.newSnapshot()) {
      Assert.assertEquals("20060101", snap.gets("1234", new Column("last", "date")));
    }

    // wait for transaction to commit
    futures.get(1).get();
    try (Snapshot snap = client.newSnapshot()) {
      Assert.assertEquals("20050101", snap.gets("6789", new Column("last", "date")));
    }

    le.close();

    // wait for transaction to commit
    futures.get(2).get();
    try (Snapshot snap = client.newSnapshot()) {
      Assert.assertEquals("20070101", snap.gets("0abc", new Column("last", "date")));
    }

    try {
      futures.get(3).get();
      Assert.fail();
    } catch (ExecutionException e) {
      // expected from future
    }
  }
}
