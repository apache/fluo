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

package io.fluo.accumulo.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.fluo.accumulo.util.ColumnConstants;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

public class SnapshotIteratorTest {
  SnapshotIterator newSI(TestData input, long startTs){
    SnapshotIterator si = new SnapshotIterator();

    Map<String, String> options = new HashMap<>();
    options.put(SnapshotIterator.TIMESTAMP_OPT, startTs+"");

    TestIteratorEnv env = new TestIteratorEnv(IteratorScope.scan);

    try {
      si.init(new SortedMapIterator(input.data), options, env);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return si;
  }

  @Test
  public void testBasic(){
    TestData input = new TestData();

    input.add("0 f q WRITE 16","11");
    input.add("0 f q DATA 11","15");

    input.add("0 f q WRITE 10","9");
    input.add("0 f q DATA 9","14");

    TestData output = new TestData(newSI(input, 6));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newSI(input, 11));
    TestData expected = new TestData().add("0 f q DATA 9","14");
    Assert.assertEquals(expected, output);

    output = new TestData(newSI(input, 17));
    expected = new TestData().add("0 f q DATA 11","15");
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testTruncation(){
    TestData input = new TestData();

    input.add("0 f q WRITE 16","11");
    input.add("0 f q DATA 11","15");

    input.add("0 f q WRITE 10","9 TRUNCATION");
    input.add("0 f q DATA 9","14");

    TestData output = new TestData(newSI(input, 6));
    TestData expected = new TestData().add("0 f q WRITE 10","9 TRUNCATION");
    Assert.assertEquals(expected, output);

    output = new TestData(newSI(input, 11));
    expected = new TestData().add("0 f q DATA 9","14");
    Assert.assertEquals(expected, output);

    output = new TestData(newSI(input, 17));
    expected = new TestData().add("0 f q DATA 11","15");
    Assert.assertEquals(expected, output);

  }

  @Test
  public void testLock(){
    TestData input = new TestData();

    input.add("0 f q WRITE 16","11");
    input.add("0 f q LOCK 21", "1 f q");
    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q DATA 11","15");

    TestData output = new TestData(newSI(input, 6));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newSI(input, 15));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newSI(input, 17));
    TestData expected = new TestData().add("0 f q DATA 11","15");
    Assert.assertEquals(expected, output);

    output = new TestData(newSI(input, 22));
    expected = new TestData().add("0 f q LOCK 21", "1 f q");
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testDelLock(){
    //sometimes commit TS is used for del lock and sometimes start TS is used... either should suppress a lock

    TestData input = new TestData();

    for(int delLockTime : new int[]{21,22}){
      input.add("0 f q DEL_LOCK "+delLockTime,"21 ROLLBACK");
      input.add("0 f q WRITE 16","11");
      input.add("0 f q LOCK 21", "1 f q");
      input.add("0 f q LOCK 11", "1 f q");
      input.add("0 f q DATA 21","16");
      input.add("0 f q DATA 11","15");

      TestData output = new TestData(newSI(input, 6));
      Assert.assertEquals(0, output.data.size());

      output = new TestData(newSI(input, 15));
      Assert.assertEquals(0, output.data.size());

      output = new TestData(newSI(input, 17));
      TestData expected = new TestData().add("0 f q DATA 11","15");
      Assert.assertEquals(expected, output);

      output = new TestData(newSI(input, 23));
      expected = new TestData().add("0 f q DATA 11","15");
      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void testSeek(){

    //data contains multiple columns and multiple rows
    TestData input = new TestData();

    input.add("0 f q1 WRITE 16","11");
    input.add("0 f q1 LOCK 21", "1 f q");
    input.add("0 f q1 LOCK 11", "1 f q");
    input.add("0 f q1 DATA 21","j");
    input.add("0 f q1 DATA 11","i");

    input.add("0 f q2 WRITE 26","20");
    input.add("0 f q2 WRITE 18","9");
    input.add("0 f q2 LOCK 20", "1 f q");
    input.add("0 f q2 LOCK 9", "1 f q");
    input.add("0 f q2 DATA 20","b");
    input.add("0 f q2 DATA 9","a");

    input.add("1 f q1 DEL_LOCK 22","21 ROLLBACK");
    input.add("1 f q1 WRITE 18","9");
    input.add("1 f q1 LOCK 21", "1 f q");
    input.add("1 f q1 LOCK 9", "1 f q");
    input.add("1 f q1 DATA 21","y");
    input.add("1 f q1 DATA 9","x");

    for(Range range : new Range[] {Range.exact("0"), Range.exact("1"), Range.exact("0","f"), Range.exact("0","f","q1"), Range.exact("0","f","q2"), Range.exact("1","f","q1")}){
      TestData output = new TestData(newSI(input, 6), range);
      Assert.assertEquals(0, output.data.size());

      output = new TestData(newSI(input, 17), range);
      TestData expected = new TestData();
      expected.addIfInRange("0 f q1 DATA 11","i", range);
      Assert.assertEquals(expected, output);

      output = new TestData(newSI(input, 19), range);
      expected = new TestData();
      expected.addIfInRange("0 f q1 DATA 11","i", range);
      expected.addIfInRange("0 f q2 DATA 9","a", range);
      expected.addIfInRange("1 f q1 DATA 9","x", range);
      Assert.assertEquals(expected, output);

      output = new TestData(newSI(input, 22), range);
      expected = new TestData();
      expected.addIfInRange("0 f q1 LOCK 21", "1 f q", range);
      expected.addIfInRange("0 f q2 DATA 9","a", range);
      expected.addIfInRange("1 f q1 DATA 9","x", range);
      Assert.assertEquals(expected, output);

      output = new TestData(newSI(input, 27), range);
      expected = new TestData();
      expected.addIfInRange("0 f q1 LOCK 21", "1 f q", range);
      expected.addIfInRange("0 f q2 DATA 20","b", range);
      expected.addIfInRange("1 f q1 DATA 9","x", range);
      Assert.assertEquals(expected, output);
    }
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNegativeTime(){
    SnapshotIterator.setSnaptime(null, -3);
  }

  @Test(expected=IllegalArgumentException.class)
  public void testNonZeroPrefix(){
    SnapshotIterator.setSnaptime(null, ColumnConstants.DATA_PREFIX | 6);
  }
}
