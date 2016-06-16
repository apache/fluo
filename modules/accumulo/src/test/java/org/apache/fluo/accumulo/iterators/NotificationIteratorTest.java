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
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

public class NotificationIteratorTest {
  NotificationIterator newNI(TestData input, IteratorScope scope, boolean fullMajc) {
    NotificationIterator ni = new NotificationIterator();

    IteratorEnvironment env = TestIteratorEnv.create(scope, fullMajc);

    try {
      Map<String, String> opts = Collections.emptyMap();
      ni.init(new SortedMapIterator(input.data), opts, env);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ni;
  }

  NotificationIterator newNI(TestData input, IteratorScope scope) {
    return newNI(input, scope, true);
  }

  private TestData getTestDataNoDeletes() {
    TestData input = new TestData();

    input.add("0 ntfy foo:bar 7", "");
    input.add("0 ntfy foo:bar 5", "");
    input.add("0 ntfy foo:bar 4", "");

    input.add("1 ntfy foo:bar 3", "");
    input.add("1 ntfy foo:bar 2", "");

    input.add("1 ntfy foo:baz 1", "");
    input.add("2 ntfy foo:baz 3", "");

    return input;
  }

  private TestData getTestData() {
    TestData input = new TestData();

    input.add("0 ntfy foo:bar 7", "");
    input.add("0 ntfy foo:bar 5 DEL", "");
    input.add("0 ntfy foo:bar 4", "");

    input.add("1 ntfy foo:bar 3", "");
    input.add("1 ntfy foo:bar 2", "");

    input.add("1 ntfy foo:baz 1", "");
    input.add("2 ntfy foo:baz 3", "");

    input.add("3 ntfy foo:baz 3 DEL", "");

    input.add("4 ntfy foo:baz 3 DEL", "");
    input.add("4 ntfy foo:baz 2", "");

    // sequence that ends with a delete
    input.add("5 ntfy foo:baz 3 DEL", "");
    input.add("5 ntfy foo:baz 2", "");
    input.add("5 ntfy foo:baz 1 DEL", "");

    // orderly pattern of del and notification
    input.add("6 ntfy foo:baz 4 DEL", "");
    input.add("6 ntfy foo:baz 3", "");
    input.add("6 ntfy foo:baz 2 DEL", "");
    input.add("6 ntfy foo:baz 1", "");

    // exact same timestamp
    input.add("7 ntfy foo:baz 3 DEL", "");
    input.add("7 ntfy foo:baz 3", "");

    // disorderly ntfy and delete pattern
    input.add("8 ntfy foo:baz 5 DEL", "");
    input.add("8 ntfy foo:baz 4", "");
    input.add("8 ntfy foo:baz 3", "");
    input.add("8 ntfy foo:baz 2 DEL", "");
    input.add("8 ntfy foo:baz 1", "");

    input.add("9 ntfy foo:bar 3", "");
    input.add("9 ntfy foo:bar 2", "");

    return input;
  }

  private TestData getMixedTestData() {
    TestData input = new TestData();

    input.add("0 ntfy zoo:bar 7", "");
    input.add("0 ntfy zoo:bar 5 DEL", "");
    input.add("0 ntfy zoo:bar 4", "");

    input.add("0 zoo bar WRITE 16", "11");
    input.add("0 zoo bar DATA 11", "15");

    input.add("1 ntfy zoo:bar 5 DEL", "");
    input.add("1 ntfy zoo:bar 4", "");

    input.add("1 zoo bar WRITE 16", "11");
    input.add("1 zoo bar DATA 11", "15");

    input.add("2 ntfy zoo:bar 5 DEL", "");
    input.add("2 ntfy zoo:bar 4", "");
    input.add("2 ntfy zoo:bar 3 DEL", "");

    input.add("2 zoo bar WRITE 16", "11");
    input.add("2 zoo bar DATA 11", "15");

    return input;
  }

  // test the behavior that is expected to be the same in all scopes
  @Test
  public void testAllScopes() {
    for (IteratorScope scope : IteratorScope.values()) {
      TestData input = getTestDataNoDeletes();

      TestData output = new TestData(newNI(input, scope));

      TestData expected = new TestData();
      expected.add("0 ntfy foo:bar 7", "");
      expected.add("1 ntfy foo:bar 3", "");
      expected.add("1 ntfy foo:baz 1", "");
      expected.add("2 ntfy foo:baz 3", "");

      Assert.assertEquals(expected, output);

      output = new TestData(newNI(input, scope), new Range("1"));
      expected = new TestData();
      expected.add("1 ntfy foo:bar 3", "");
      expected.add("1 ntfy foo:baz 1", "");

      Assert.assertEquals(expected, output);

      output = new TestData(newNI(input, scope), new Range("0", "1"));
      expected = new TestData();
      expected.add("0 ntfy foo:bar 7", "");
      expected.add("1 ntfy foo:bar 3", "");
      expected.add("1 ntfy foo:baz 1", "");

      Assert.assertEquals(expected, output);

      output = new TestData(newNI(input, scope), new Range("1", "2"));
      expected = new TestData();
      expected.add("1 ntfy foo:bar 3", "");
      expected.add("1 ntfy foo:baz 1", "");
      expected.add("2 ntfy foo:baz 3", "");

      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void testScans() {
    TestData input = getTestData();

    // check the test code
    Assert.assertEquals(26, input.data.size());

    for (Key key : input.data.keySet()) {
      TestData output =
          new TestData(newNI(input, IteratorScope.scan), new Range(key, false, null, true));
      TestData expected = null;
      if (key.getRowData().toString().equals("0")) {
        expected = new TestData();
        expected.add("1 ntfy foo:bar 3", "");
        expected.add("1 ntfy foo:baz 1", "");
        expected.add("2 ntfy foo:baz 3", "");
        expected.add("9 ntfy foo:bar 3", "");
      } else if (key.getRowData().toString().equals("1")) {
        if (key.getColumnQualifierData().toString().equals("foo:bar")) {
          expected = new TestData();
          expected.add("1 ntfy foo:baz 1", "");
          expected.add("2 ntfy foo:baz 3", "");
          expected.add("9 ntfy foo:bar 3", "");
        } else {
          expected = new TestData();
          expected.add("2 ntfy foo:baz 3", "");
          expected.add("9 ntfy foo:bar 3", "");
        }
      } else if (key.getRowData().toString().compareTo("9") < 0) {
        expected = new TestData();
        expected.add("9 ntfy foo:bar 3", "");
      } else {
        expected = new TestData();
      }

      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void testPartialCompaction() {
    for (IteratorScope scope : Arrays.asList(IteratorScope.minc, IteratorScope.majc)) {

      TestData input = getTestData();

      TestData output = new TestData(newNI(input, scope, false));

      TestData expected = new TestData();
      expected.add("0 ntfy foo:bar 7", "");
      expected.add("1 ntfy foo:bar 3", "");
      expected.add("1 ntfy foo:baz 1", "");
      expected.add("2 ntfy foo:baz 3", "");
      expected.add("3 ntfy foo:baz 3 DEL", "");
      expected.add("5 ntfy foo:baz 3 DEL", "");
      expected.add("8 ntfy foo:baz 5 DEL", "");
      expected.add("9 ntfy foo:bar 3", "");

      Assert.assertEquals(expected, output);

      input = getMixedTestData();

      expected = new TestData();

      expected.add("0 ntfy zoo:bar 7", "");

      expected.add("0 zoo bar WRITE 16", "11");
      expected.add("0 zoo bar DATA 11", "15");

      expected.add("1 zoo bar WRITE 16", "11");
      expected.add("1 zoo bar DATA 11", "15");

      expected.add("2 ntfy zoo:bar 5 DEL", "");

      expected.add("2 zoo bar WRITE 16", "11");
      expected.add("2 zoo bar DATA 11", "15");

      output = new TestData(newNI(input, scope, false));
      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void testFullCompaction() {
    IteratorScope scope = IteratorScope.majc;

    TestData input = getTestData();

    TestData output = new TestData(newNI(input, scope, true));

    TestData expected = new TestData();
    expected.add("0 ntfy foo:bar 7", "");
    expected.add("1 ntfy foo:bar 3", "");
    expected.add("1 ntfy foo:baz 1", "");
    expected.add("2 ntfy foo:baz 3", "");
    expected.add("9 ntfy foo:bar 3", "");

    Assert.assertEquals(expected, output);

    input = getMixedTestData();

    expected = new TestData();

    expected.add("0 ntfy zoo:bar 7", "");

    expected.add("0 zoo bar WRITE 16", "11");
    expected.add("0 zoo bar DATA 11", "15");

    expected.add("1 zoo bar WRITE 16", "11");
    expected.add("1 zoo bar DATA 11", "15");

    expected.add("2 zoo bar WRITE 16", "11");
    expected.add("2 zoo bar DATA 11", "15");

    output = new TestData(newNI(input, scope, true));
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testLots() {
    IteratorScope scope = IteratorScope.scan;

    Random rand = new Random(42L);

    TestData input = new TestData();
    for (int i = 0; i < 1000; i++) {
      int maxTs = Math.max(1, rand.nextInt(50));
      for (int j = 0; j < maxTs; j++) {
        input.add("row" + i + " ntfy foo:baz " + j, "");
      }
    }

    TestData output = new TestData(newNI(input, scope, true));

    rand = new Random(42L);
    TestData expected = new TestData();
    for (int i = 0; i < 1000; i++) {
      int maxTs = Math.max(1, rand.nextInt(50)) - 1;
      expected.add("row" + i + " ntfy foo:baz " + maxTs, "");
    }

    Assert.assertEquals(expected, output);
  }
}
