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

package org.apache.fluo.accumulo.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.junit.Assert;
import org.junit.Test;

public class SnapshotIteratorTest {
  SnapshotIterator newSI(TestData input, long startTs) {
    SnapshotIterator si = new SnapshotIterator();

    Map<String, String> options = new HashMap<>();
    options.put(SnapshotIterator.TIMESTAMP_OPT, startTs + "");

    IteratorEnvironment env = TestIteratorEnv.create(IteratorScope.scan, true);

    try {
      SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(input.data);
      si.init(source, options, env);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return si;
  }

  @Test
  public void testBasic() {
    TestData input = new TestData();

    input.add("0 f q WRITE 16", "11");
    input.add("0 f q DATA 11", "15");

    input.add("0 f q WRITE 10", "9");
    input.add("0 f q DATA 9", "14");

    TestData output = new TestData(newSI(input, 6));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newSI(input, 11));
    TestData expected = new TestData().add("0 f q DATA 9", "14");
    Assert.assertEquals(expected, output);

    output = new TestData(newSI(input, 17));
    expected = new TestData().add("0 f q DATA 11", "15");
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testLock() {
    TestData input = new TestData();

    input.add("0 f q WRITE 16", "11");
    input.add("0 f q LOCK 21", "1 f q");
    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q DATA 11", "15");

    TestData output = new TestData(newSI(input, 6));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newSI(input, 15));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newSI(input, 17));
    TestData expected = new TestData().add("0 f q DATA 11", "15");
    Assert.assertEquals(expected, output);

    output = new TestData(newSI(input, 22));
    expected = new TestData().add("0 f q LOCK 21", "1 f q");
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testDelLock() {
    // sometimes commit TS is used for del lock and sometimes start TS is used... either should
    // suppress a lock

    TestData input = new TestData();


    input.add("0 f q DEL_LOCK 21", "0 ROLLBACK");
    input.add("0 f q WRITE 16", "11");
    input.add("0 f q LOCK 21", "1 f q");
    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q DATA 21", "16");
    input.add("0 f q DATA 11", "15");

    TestData output = new TestData(newSI(input, 6));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newSI(input, 15));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newSI(input, 17));
    TestData expected = new TestData().add("0 f q DATA 11", "15");
    Assert.assertEquals(expected, output);

    output = new TestData(newSI(input, 23));
    expected = new TestData().add("0 f q DATA 11", "15");
    Assert.assertEquals(expected, output);


    // test case where there is newer lock thats not invalidated by DEL_LOCK
    input = new TestData();
    input.add("0 f q DEL_LOCK 18", "0 ABORT");
    input.add("0 f q WRITE 16", "11");
    input.add("0 f q LOCK 21", "1 f q");
    input.add("0 f q LOCK 18", "1 f q");
    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q DATA 21", "16");
    input.add("0 f q DATA 11", "15");

    output = new TestData(newSI(input, 6));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newSI(input, 17));
    expected = new TestData().add("0 f q DATA 11", "15");
    Assert.assertEquals(expected, output);

    output = new TestData(newSI(input, 19));
    expected = new TestData().add("0 f q DATA 11", "15");
    Assert.assertEquals(expected, output);

    output = new TestData(newSI(input, 23));
    expected = new TestData().add("0 f q LOCK 21", "1 f q");
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testSeek() {

    // data contains multiple columns and multiple rows
    TestData input = new TestData();

    input.add("0 f q1 WRITE 16", "11");
    input.add("0 f q1 LOCK 21", "1 f q");
    input.add("0 f q1 LOCK 11", "1 f q");
    input.add("0 f q1 DATA 21", "j");
    input.add("0 f q1 DATA 11", "i");

    input.add("0 f q2 WRITE 26", "20");
    input.add("0 f q2 WRITE 18", "9");
    input.add("0 f q2 LOCK 20", "1 f q");
    input.add("0 f q2 LOCK 9", "1 f q");
    input.add("0 f q2 DATA 20", "b");
    input.add("0 f q2 DATA 9", "a");

    input.add("1 f q1 DEL_LOCK 21", "0 ROLLBACK");
    input.add("1 f q1 WRITE 18", "9");
    input.add("1 f q1 LOCK 21", "1 f q");
    input.add("1 f q1 LOCK 9", "1 f q");
    input.add("1 f q1 DATA 21", "y");
    input.add("1 f q1 DATA 9", "x");

    for (Range range : new Range[] {Range.exact("0"), Range.exact("1"), Range.exact("0", "f"),
        Range.exact("0", "f", "q1"), Range.exact("0", "f", "q2"), Range.exact("1", "f", "q1")}) {
      TestData output = new TestData(newSI(input, 6), range);
      Assert.assertEquals(0, output.data.size());

      output = new TestData(newSI(input, 17), range);
      TestData expected = new TestData();
      expected.addIfInRange("0 f q1 DATA 11", "i", range);
      Assert.assertEquals(expected, output);

      output = new TestData(newSI(input, 19), range);
      expected = new TestData();
      expected.addIfInRange("0 f q1 DATA 11", "i", range);
      expected.addIfInRange("0 f q2 DATA 9", "a", range);
      expected.addIfInRange("1 f q1 DATA 9", "x", range);
      Assert.assertEquals(expected, output);

      output = new TestData(newSI(input, 22), range);
      expected = new TestData();
      expected.addIfInRange("0 f q1 LOCK 21", "1 f q", range);
      expected.addIfInRange("0 f q2 DATA 9", "a", range);
      expected.addIfInRange("1 f q1 DATA 9", "x", range);
      Assert.assertEquals(expected, output);

      output = new TestData(newSI(input, 27), range);
      expected = new TestData();
      expected.addIfInRange("0 f q1 LOCK 21", "1 f q", range);
      expected.addIfInRange("0 f q2 DATA 20", "b", range);
      expected.addIfInRange("1 f q1 DATA 9", "x", range);
      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void testColumnsWithManyWrites() throws Exception {
    TestData input = new TestData();

    int numToWrite = 1000;

    for (int i = 0; i < numToWrite * 3; i += 3) {
      int commitTime = i + 1;
      int startTime = i;
      int val1 = ("" + i).hashCode();
      int val2 = ("" + val1).hashCode();

      input.add("0 f q1 WRITE " + commitTime, "" + startTime);
      input.add("0 f q1 LOCK " + startTime, "1 f q");
      input.add("0 f q1 DATA " + startTime, "" + val1);

      input.add("1 f q1 TX_DONE " + commitTime, "" + startTime);
      input.add("1 f q1 WRITE " + commitTime, "" + startTime);
      input.add("1 f q1 LOCK " + startTime, "1 f q");
      input.add("1 f q1 DATA " + startTime, "" + val2);
    }

    Range[] ranges =
        new Range[] {new Range(), Range.exact("0", "f", "q1"), Range.exact("1", "f", "q1"),
            Range.exact("2", "f", "q1")};


    for (Range range : ranges) {
      checkManyColumnData(input, numToWrite, range);
    }

    // add locks
    int startTime = numToWrite * 3;
    input.add("1 f q1 LOCK " + startTime, "1 f q");
    input.add("1 f q1 DATA " + startTime, "foo");
    TestData output = new TestData(newSI(input, startTime + 1), new Range());
    TestData expected = new TestData();
    expected.add("1 f q1 LOCK " + startTime, "1 f q");
    startTime -= 3;
    expected.add("0 f q1 DATA " + startTime, ("" + startTime).hashCode() + "");
    Assert.assertEquals(expected, output);

    for (Range range : ranges) {
      checkManyColumnData(input, numToWrite, range);
    }

  }

  private void checkManyColumnData(TestData input, int numToWrite, Range range) throws IOException {
    for (int i = numToWrite * 3 - 1; i > 3; i -= 3) {
      TestData output = new TestData(newSI(input, i), range);
      TestData expected = new TestData();
      // snapshot time of commited transaction
      int st = i - 2;
      int val1 = ("" + st).hashCode();
      int val2 = ("" + val1).hashCode();
      expected.addIfInRange("0 f q1 DATA " + st, val1 + "", range);
      expected.addIfInRange("1 f q1 DATA " + st, val2 + "", range);
      Assert.assertEquals(expected, output);
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNegativeTime() {
    SnapshotIterator.setSnaptime(null, -3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testNonZeroPrefix() {
    SnapshotIterator.setSnaptime(null, ColumnConstants.DATA_PREFIX | 6);
  }
}
