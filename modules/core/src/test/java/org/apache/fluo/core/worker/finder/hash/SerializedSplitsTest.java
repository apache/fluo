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

package org.apache.fluo.core.worker.finder.hash;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.fluo.api.data.Bytes;
import org.junit.Assert;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class SerializedSplitsTest {
  @Test
  public void testLotsOfSplits() {
    List<Bytes> splits = IntStream.iterate(0, i -> i + 13).limit(1_000_000)
        .mapToObj(i -> String.format("%08x", i)).map(Bytes::of).collect(toList());

    byte[] data = SerializedSplits.serialize(splits);
    Assert.assertTrue(data.length <= SerializedSplits.MAX_SIZE);

    List<Bytes> splits2 = new ArrayList<>();
    SerializedSplits.deserialize(splits2::add, data);

    Assert.assertTrue(splits2.size() > 10_000);
    Assert.assertTrue(splits2.size() < splits.size());

    int expectedDiff = Integer.parseInt(splits2.get(1).toString(), 16)
        - Integer.parseInt(splits2.get(0).toString(), 16);
    Assert.assertTrue(expectedDiff > 13);
    Assert.assertTrue(expectedDiff % 13 == 0);
    // check that splits are evenly spaced
    for (int i = 1; i < splits2.size(); i++) {
      int sp1 = Integer.parseInt(splits2.get(i - 1).toString(), 16);
      int sp2 = Integer.parseInt(splits2.get(i).toString(), 16);
      Assert.assertEquals(expectedDiff, sp2 - sp1);
      Assert.assertEquals(0, sp1 % 13);
      Assert.assertEquals(0, sp2 % 13);
    }

    Assert.assertTrue(splits.get(0).compareTo(splits2.get(0)) <= 0);
    Assert
        .assertTrue(splits.get(splits.size() - 1).compareTo(splits2.get(splits2.size() - 1)) >= 0);
  }

  @Test
  public void testSimple() {
    Set<Bytes> splits = IntStream.iterate(0, i -> i + 13).limit(1_000)
        .mapToObj(i -> String.format("%08x", i)).map(Bytes::of).collect(toSet());

    byte[] data = SerializedSplits.serialize(splits);

    HashSet<Bytes> splits2 = new HashSet<>();
    SerializedSplits.deserialize(splits2::add, data);

    Assert.assertEquals(splits, splits2);
  }
}
