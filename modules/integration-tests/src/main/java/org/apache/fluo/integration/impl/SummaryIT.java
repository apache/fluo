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

import java.util.List;

import org.apache.accumulo.core.client.summary.Summary;
import org.apache.fluo.accumulo.summarizer.FluoSummarizer;
import org.apache.fluo.accumulo.summarizer.FluoSummarizer.Counts;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.integration.ITBaseImpl;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SummaryIT extends ITBaseImpl {

  @Test
  public void testSummaries() throws Exception {
    try (Transaction tx = client.newTransaction()) {
      String seen = tx.withReadLock().gets("u:http://wikipedia.com/abc", new Column("doc", "seen"));
      if (seen == null) {
        tx.set("d:7705", new Column("doc", "source"), "http://wikipedia.com/abc");
      }
      tx.commit();
    }

    List<Summary> summaries = aClient.tableOperations().summaries(table).flush(true).retrieve();

    Counts counts = FluoSummarizer.getCounts(summaries.get(0));

    assertEquals(0, counts.ack);
    assertEquals(1, counts.data);
    assertEquals(0, counts.delLock);
    assertEquals(1, counts.delrlock);
    assertEquals(0, counts.lock);
    assertEquals(0, counts.ntfy);
    assertEquals(0, counts.ntfyDel);
    assertEquals(0, counts.rlock);
    assertEquals(1, counts.txDone);
    assertEquals(1, counts.write);

    try (Transaction tx = client.newTransaction()) {
      tx.set("d:7705", new Column("doc", "source"), "http://wikipedia.com/abcd");
      tx.commit();
    }

    summaries = aClient.tableOperations().summaries(table).flush(true).retrieve();

    counts = FluoSummarizer.getCounts(summaries.get(0));

    assertEquals(0, counts.ack);
    assertEquals(2, counts.data);
    assertEquals(0, counts.delLock);
    assertEquals(1, counts.delrlock);
    assertEquals(0, counts.lock);
    assertEquals(0, counts.ntfy);
    assertEquals(0, counts.ntfyDel);
    assertEquals(0, counts.rlock);
    assertEquals(2, counts.txDone);
    assertEquals(2, counts.write);
  }
}
