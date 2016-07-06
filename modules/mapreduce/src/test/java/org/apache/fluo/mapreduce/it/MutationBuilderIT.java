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

package org.apache.fluo.mapreduce.it;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.apache.fluo.mapreduce.FluoMutationGenerator;
import org.junit.Assert;
import org.junit.Test;

public class MutationBuilderIT extends ITBaseImpl {

  @Test
  public void testBatchWrite() throws Exception {
    // test initializing a Fluo table by batch writing to it

    // use a batch writer to test this because its easier than using AccumuloOutputFormat
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    try {

      FluoMutationGenerator mb1 = new FluoMutationGenerator(Bytes.of("row1"));
      mb1.put(new Column("cf1", "cq1"), Bytes.of("v1"));
      mb1.put(new Column("cf1", "cq2"), Bytes.of("v2"));
      mb1.put(new Column("cf1", "cq3"), Bytes.of("v3"));

      bw.addMutation(mb1.build());

      FluoMutationGenerator mb2 = new FluoMutationGenerator(Bytes.of("row2"));
      mb2.put(new Column("cf1", "cq1"), Bytes.of("v4"));
      mb2.put(new Column("cf1", "cq2"), Bytes.of("v5"));

      bw.addMutation(mb2.build());

    } finally {
      bw.close();
    }

    TestTransaction tx1 = new TestTransaction(env);
    TestTransaction tx2 = new TestTransaction(env);

    Assert.assertEquals("v1", tx1.gets("row1", new Column("cf1", "cq1")));
    Assert.assertEquals("v2", tx1.gets("row1", new Column("cf1", "cq2")));
    Assert.assertEquals("v3", tx1.gets("row1", new Column("cf1", "cq3")));
    Assert.assertEquals("v4", tx1.gets("row2", new Column("cf1", "cq1")));
    Assert.assertEquals("v5", tx1.gets("row2", new Column("cf1", "cq2")));

    tx1.set("row1", new Column("cf1", "cq2"), "v6");
    tx1.delete("row1", new Column("cf1", "cq3"));
    tx1.set("row2", new Column("cf1", "cq2"), "v7");

    tx1.done();

    // tx2 should see not changes from tx1
    Assert.assertEquals("v1", tx2.gets("row1", new Column("cf1", "cq1")));
    Assert.assertEquals("v2", tx2.gets("row1", new Column("cf1", "cq2")));
    Assert.assertEquals("v3", tx2.gets("row1", new Column("cf1", "cq3")));
    Assert.assertEquals("v4", tx2.gets("row2", new Column("cf1", "cq1")));
    Assert.assertEquals("v5", tx2.gets("row2", new Column("cf1", "cq2")));

    TestTransaction tx3 = new TestTransaction(env);

    // should see changes from tx1
    Assert.assertEquals("v1", tx3.gets("row1", new Column("cf1", "cq1")));
    Assert.assertEquals("v6", tx3.gets("row1", new Column("cf1", "cq2")));
    Assert.assertNull(tx3.gets("row1", new Column("cf1", "cq3")));
    Assert.assertEquals("v4", tx3.gets("row2", new Column("cf1", "cq1")));
    Assert.assertEquals("v7", tx3.gets("row2", new Column("cf1", "cq2")));
  }
}
