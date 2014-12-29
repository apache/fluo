/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.mapreduce;

import io.fluo.core.TestTransaction;

import io.fluo.core.TestBaseImpl;
import io.fluo.mapreduce.MutationBuilder;
import io.fluo.api.data.Bytes;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.junit.Assert;
import org.junit.Test;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;

public class MutationBuilderIT extends TestBaseImpl {

  static final TypeLayer tl = new TypeLayer(new StringEncoder());

  @Test
  public void testBatchWrite() throws Exception {
    // test initializing a Fluo table by batch writing to it

    // use a batch writer to test this because its easier than using AccumuloOutputFormat
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    try {

      MutationBuilder mb1 = new MutationBuilder(Bytes.of("row1"));
      mb1.put(tl.bc().fam("cf1").qual("cq1").vis(), Bytes.of("v1"));
      mb1.put(tl.bc().fam("cf1").qual("cq2").vis(), Bytes.of("v2"));
      mb1.put(tl.bc().fam("cf1").qual("cq3").vis(), Bytes.of("v3"));

      bw.addMutation(mb1.build());

      MutationBuilder mb2 = new MutationBuilder(Bytes.of("row2"));
      mb2.put(tl.bc().fam("cf1").qual("cq1").vis(), Bytes.of("v4"));
      mb2.put(tl.bc().fam("cf1").qual("cq2").vis(), Bytes.of("v5"));

      bw.addMutation(mb2.build());

    } finally {
      bw.close();
    }

    TestTransaction tx1 = new TestTransaction(env);
    TestTransaction tx2 = new TestTransaction(env);

    Assert.assertEquals("v1", tx1.get().row("row1").fam("cf1").qual("cq1").toString());
    Assert.assertEquals("v2", tx1.get().row("row1").fam("cf1").qual("cq2").toString());
    Assert.assertEquals("v3", tx1.get().row("row1").fam("cf1").qual("cq3").toString());
    Assert.assertEquals("v4", tx1.get().row("row2").fam("cf1").qual("cq1").toString());
    Assert.assertEquals("v5", tx1.get().row("row2").fam("cf1").qual("cq2").toString());

    tx1.mutate().row("row1").fam("cf1").qual("cq2").set("v6");
    tx1.mutate().row("row1").fam("cf1").qual("cq3").delete();
    tx1.mutate().row("row2").fam("cf1").qual("cq2").set("v7");

    tx1.done();

    // tx2 should see not changes from tx1
    Assert.assertEquals("v1", tx2.get().row("row1").fam("cf1").qual("cq1").toString());
    Assert.assertEquals("v2", tx2.get().row("row1").fam("cf1").qual("cq2").toString());
    Assert.assertEquals("v3", tx2.get().row("row1").fam("cf1").qual("cq3").toString());
    Assert.assertEquals("v4", tx2.get().row("row2").fam("cf1").qual("cq1").toString());
    Assert.assertEquals("v5", tx2.get().row("row2").fam("cf1").qual("cq2").toString());

    TestTransaction tx3 = new TestTransaction(env);

    // should see changes from tx1
    Assert.assertEquals("v1", tx3.get().row("row1").fam("cf1").qual("cq1").toString());
    Assert.assertEquals("v6", tx3.get().row("row1").fam("cf1").qual("cq2").toString());
    Assert.assertNull(tx3.get().row("row1").fam("cf1").qual("cq3").toString());
    Assert.assertEquals("v4", tx3.get().row("row2").fam("cf1").qual("cq1").toString());
    Assert.assertEquals("v7", tx3.get().row("row2").fam("cf1").qual("cq2").toString());
  }
}
