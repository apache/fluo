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
import java.util.Collections;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

public class OpenReadLockIteratorTest {
  OpenReadLockIterator newORLI(TestData input) {
    OpenReadLockIterator si = new OpenReadLockIterator();

    IteratorEnvironment env = TestIteratorEnv.create(IteratorScope.scan, true);

    try {
      SortedKeyValueIterator<Key, Value> source = new SortedMapIterator(input.data);
      si.init(source, Collections.emptyMap(), env);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return si;
  }

  @Test
  public void testBasic() {
    TestData input = new TestData();

    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q WRITE 16", "11");
    input.add("0 f q DATA 11", "15");
    input.add("0 f q DEL_RLOCK 25", "36");
    input.add("0 f q RLOCK 25", " 0 f q");
    input.add("0 f q RLOCK 23", " 0 f q");
    input.add("0 f q DEL_RLOCK 21", "30");
    input.add("0 f q RLOCK 21", " 0 f q");

    input.add("1 f q1 TX_DONE 16", "11");
    input.add("1 f q LOCK 11", "1 f q");
    input.add("1 f q WRITE 16", "11");
    input.add("1 f q DATA 11", "15");
    input.add("1 f q RLOCK 25", " 0 f q");
    input.add("1 f q RLOCK 23", " 0 f q");
    input.add("1 f q DEL_RLOCK 21", "30");
    input.add("1 f q RLOCK 21", " 0 f q");
    input.add("1 f q ACK 11", "");

    input.add("3 f q LOCK 11", "1 f q");
    input.add("3 f q WRITE 16", "11");
    input.add("3 f q DATA 11", "15");
    input.add("3 f q DEL_RLOCK 25", "36");
    input.add("3 f q RLOCK 25", " 0 f q");
    input.add("3 f q DEL_RLOCK 23", "33");
    input.add("3 f q RLOCK 23", " 0 f q");
    input.add("3 f q DEL_RLOCK 21", "30");
    input.add("3 f q RLOCK 21", " 0 f q");

    TestData expected = new TestData();
    expected.add("0 f q RLOCK 23", " 0 f q");
    expected.add("1 f q RLOCK 25", " 0 f q");
    expected.add("1 f q RLOCK 23", " 0 f q");

    TestData output = new TestData(newORLI(input));
    Assert.assertEquals(expected, output);

    output = new TestData(newORLI(input), new Range(), true);
    Assert.assertEquals(expected, output);
  }
}
