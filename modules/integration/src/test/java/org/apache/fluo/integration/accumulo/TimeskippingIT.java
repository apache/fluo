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

package org.apache.fluo.integration.accumulo;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.integration.ITBase;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeskippingIT extends ITBase {

  private static final Logger log = LoggerFactory.getLogger(TimeskippingIT.class);

  @Test
  public void testTimestampSkippingIterPerformance() throws Exception {

    conn.tableOperations().create("ttsi", false);

    BatchWriter bw = conn.createBatchWriter("ttsi", new BatchWriterConfig());
    Mutation m = new Mutation("r1");
    for (int i = 0; i < 100000; i++) {
      m.put("f1", "q1", i, "v" + i);
    }

    bw.addMutation(m);
    bw.close();

    long t2 = System.currentTimeMillis();

    Scanner scanner = conn.createScanner("ttsi", Authorizations.EMPTY);
    scanner.addScanIterator(new IteratorSetting(10, Skip100StampsIterator.class));

    Assert.assertEquals("999 1000", Iterables.getOnlyElement(scanner).getValue().toString());
    long t3 = System.currentTimeMillis();

    if (t3 - t2 > 3000) {
      log.warn("Timestamp skipping iterator took longer than expected " + (t3 - t2));
    }

    conn.tableOperations().flush("ttsi", null, null, true);

    long t4 = System.currentTimeMillis();
    Assert.assertEquals("999 1000", Iterables.getOnlyElement(scanner).getValue().toString());
    long t5 = System.currentTimeMillis();

    if (t5 - t4 > 3000) {
      log.warn("Timestamp skipping iterator took longer than expected " + (t5 - t4));
    }
  }
}
