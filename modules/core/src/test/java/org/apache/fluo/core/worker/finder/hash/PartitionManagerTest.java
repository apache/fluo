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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IntSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import org.apache.fluo.api.data.Bytes;
import org.junit.Assert;
import org.junit.Test;

import static java.util.stream.Collectors.toList;

public class PartitionManagerTest {

  @Test
  public void testGrouping() {
    IntFunction<String> nff = i -> String.format("f-%04d", i);

    for (int numSplits : new int[] {1, 10, 100, 1000}) {
      for (int numWorkers : new int[] {1, 5, 10, 11, 100}) {
        for (int groupSize : new int[] {1, 2, 3, 5, 7, 13, 17, 19, 43, 73, 97}) {
          int expectedGroups = Math.max(1, numWorkers / groupSize);
          int maxGroupSize = Math.min(numWorkers,
              groupSize + (int) Math.ceil((numWorkers % groupSize) / (double) expectedGroups));

          TreeSet<String> children = new TreeSet<>();

          IntStream.range(0, numWorkers).mapToObj(nff).forEach(children::add);

          Collection<Bytes> rows = IntStream.iterate(0, i -> i + 1000).limit(numSplits)
              .mapToObj(i -> String.format("r%06d", i)).map(Bytes::of).collect(toList());
          Collection<TableRange> tablets = TableRange.toTabletRanges(rows);

          Set<String> idCombos = new HashSet<>();
          Map<Integer, RangeSet> groupTablets = new HashMap<>();

          for (int i = 0; i < numWorkers; i++) {
            String me = nff.apply(i);
            PartitionInfo pi = PartitionManager.getGroupInfo(me, children, tablets, groupSize);
            Assert.assertEquals(expectedGroups, pi.getNumGroups());
            Assert.assertTrue(pi.getMyGroupSize() >= Math.min(numWorkers, groupSize)
                && pi.getMyGroupSize() <= maxGroupSize);
            Assert.assertEquals(numWorkers, pi.getNumWorkers());
            Assert.assertTrue(pi.getMyIdInGroup() >= 0 && pi.getMyIdInGroup() < maxGroupSize);
            Assert.assertTrue(pi.getMyGroupId() >= 0 && pi.getMyGroupId() < expectedGroups);

            Assert.assertFalse(idCombos.contains(pi.getMyGroupId() + ":" + pi.getMyIdInGroup()));
            idCombos.add(pi.getMyGroupId() + ":" + pi.getMyIdInGroup());

            if (!groupTablets.containsKey(pi.getMyGroupId())) {
              groupTablets.put(pi.getMyGroupId(), pi.getMyGroupsRanges());
            } else {
              Assert.assertEquals(groupTablets.get(pi.getMyGroupId()), pi.getMyGroupsRanges());
            }
          }

          Assert.assertEquals(numWorkers, idCombos.size());

          // check that the tablets for each group are disjoint and that the union of the tablets
          // for each group has all tablets
          HashSet<TableRange> allTabletsFromGroups = new HashSet<>();

          for (RangeSet tabletSet : groupTablets.values()) {
            tabletSet.forEach(tr -> {
              Assert.assertFalse(allTabletsFromGroups.contains(tr));
              allTabletsFromGroups.add(tr);
            });
          }

          Assert.assertEquals(new HashSet<>(tablets), allTabletsFromGroups);

          // check that all groups have about the same number of tablets
          IntSummaryStatistics summaryStats =
              groupTablets.values().stream().mapToInt(RangeSet::size).summaryStatistics();
          Assert.assertTrue(summaryStats.getMax() - summaryStats.getMin() < 2);
        }
      }
    }
  }
}
