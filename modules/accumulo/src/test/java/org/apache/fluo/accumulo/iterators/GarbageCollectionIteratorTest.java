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

import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

public class GarbageCollectionIteratorTest {

  GarbageCollectionIterator newGCI(TestData input, long oldestActive) {
    return newGCI(input, oldestActive, true);
  }

  GarbageCollectionIterator newGCI(TestData input, long oldestActive, boolean fullMajc) {
    GarbageCollectionIterator gci = new GarbageCollectionIterator();
    Map<String, String> options = new HashMap<>();
    options.put(GarbageCollectionIterator.GC_TIMESTAMP_OPT, Long.toString(oldestActive));
    IteratorEnvironment env = TestIteratorEnv.create(IteratorScope.majc, fullMajc);

    try {
      gci.init(new SortedMapIterator(input.data), options, env);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return gci;
  }

  @Test
  public void testSimple() {
    TestData input = new TestData();

    input.add("0 f q WRITE 16", "11");
    input.add("0 f q DATA 11", "15");

    TestData output = new TestData(newGCI(input, 10));

    TestData expected = new TestData();
    expected.add("0 f q WRITE 16", "11");
    expected.add("0 f q DATA 11", "15");

    Assert.assertEquals(expected, output);

    output = new TestData(newGCI(input, 60));

    expected = new TestData();
    expected.add("0 f q WRITE 16", "11");
    expected.add("0 f q DATA 11", "15");

    Assert.assertEquals(expected, output);
  }

  @Test
  public void testAckRemoval() {

    TestData input = new TestData();
    // TODO need to test w/ more types
    input.add("0 f q TX_DONE 21", "");
    input.add("0 f q WRITE 21", "20 PRIMARY");
    input.add("0 f q ACK 20", "");
    input.add("0 f q ACK 16", "");
    input.add("0 f q ACK 14", "");

    TestData expected = new TestData();
    expected.add("0 f q TX_DONE 21", "");
    expected.add("0 f q WRITE 21", "20 PRIMARY");

    TestData output = new TestData(newGCI(input, 5));

    Assert.assertEquals(2, expected.data.size());
    Assert.assertEquals(expected, output);

    input = new TestData();
    input.add("0 f q TX_DONE 21", "");
    input.add("0 f q ACK 20", "");
    input.add("0 f q ACK 16", "");
    input.add("0 f q ACK 14", "");

    expected = new TestData();
    expected.add("0 f q TX_DONE 21", "");
    expected.add("0 f q ACK 20", "");

    output = new TestData(newGCI(input, 5));

    Assert.assertEquals(2, expected.data.size());
    Assert.assertEquals(expected, output);

    input = new TestData();
    input.add("0 f q WRITE 19", "16 PRIMARY");
    input.add("0 f q ACK 20", "");
    input.add("0 f q ACK 16", "");
    input.add("0 f q ACK 14", "");

    expected = new TestData();
    expected.add("0 f q WRITE 19", "16 PRIMARY");
    expected.add("0 f q ACK 20", "");

    output = new TestData(newGCI(input, 5));

    Assert.assertEquals(2, expected.data.size());
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testDataRemoval() {
    TestData input = new TestData();

    input.add("0 f q WRITE 52", "48");
    input.add("0 f q WRITE 45", "41");
    input.add("0 f q WRITE 38", "33");
    input.add("0 f q WRITE 20", "17");
    input.add("0 f q DATA 48", "18");
    input.add("0 f q DATA 41", "15");
    input.add("0 f q DATA 33", "13");
    input.add("0 f q DATA 17", "11");

    TestData output = new TestData(newGCI(input, 3));
    Assert.assertEquals(8, input.data.size());
    Assert.assertEquals(input, output);

    // this time falls between the start and commit times of one of the transactions that wrote data
    output = new TestData(newGCI(input, 35));
    Assert.assertEquals(input, output);

    TestData expected = new TestData();
    expected.add("0 f q WRITE 52", "48");
    expected.add("0 f q WRITE 45", "41");
    expected.add("0 f q WRITE 38", "33");
    expected.add("0 f q DATA 48", "18");
    expected.add("0 f q DATA 41", "15");
    expected.add("0 f q DATA 33", "13");

    output = new TestData(newGCI(input, 39));
    Assert.assertEquals(6, expected.data.size());
    Assert.assertEquals(expected, output);

    expected = new TestData();
    expected.add("0 f q WRITE 52", "48");
    expected.add("0 f q WRITE 45", "41");
    expected.add("0 f q DATA 48", "18");
    expected.add("0 f q DATA 41", "15");

    output = new TestData(newGCI(input, 46));
    Assert.assertEquals(4, expected.data.size());
    Assert.assertEquals(expected, output);

    expected = new TestData();
    expected.add("0 f q WRITE 52", "48");
    expected.add("0 f q DATA 48", "18");

    output = new TestData(newGCI(input, 53));
    Assert.assertEquals(2, expected.data.size());
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testTxDone() {

    TestData inputBase = new TestData();

    inputBase.add("0 f q WRITE 52", "48");
    inputBase.add("0 f q WRITE 45", "41");
    inputBase.add("0 f q WRITE 38", "33");
    inputBase.add("0 f q DATA 48", "18");
    inputBase.add("0 f q DATA 41", "15");
    inputBase.add("0 f q DATA 33", "13");
    inputBase.add("0 f q DATA 20", "11");

    // test with primary and txdone
    TestData input = new TestData(inputBase);

    input.add("0 f q TX_DONE 20", "");
    input.add("0 f q WRITE 20", "17 PRIMARY");

    TestData output = new TestData(newGCI(input, 60));

    TestData expected = new TestData();
    expected.add("0 f q WRITE 52", "48");
    expected.add("0 f q DATA 48", "18");

    Assert.assertEquals(expected, output);

    // test with only primary
    input = new TestData(inputBase);

    input.add("0 f q WRITE 20", "17 PRIMARY");

    output = new TestData(newGCI(input, 60));

    expected = new TestData();
    expected.add("0 f q WRITE 52", "48");
    expected.add("0 f q WRITE 20", "17 PRIMARY");
    expected.add("0 f q DATA 48", "18");

    Assert.assertEquals(expected, output);

    // test with only txdone
    input = new TestData(inputBase);

    input.add("0 f q TX_DONE 20", "");

    output = new TestData(newGCI(input, 60));

    expected = new TestData();
    expected.add("0 f q TX_DONE 20", "");
    expected.add("0 f q WRITE 52", "48");
    expected.add("0 f q DATA 48", "18");

    Assert.assertEquals(expected, output);

    // test TX_DONE and WRITE where times do not match
    input = new TestData(inputBase);

    input.add("0 f q TX_DONE 17", "");
    input.add("0 f q WRITE 20", "17 PRIMARY");

    output = new TestData(newGCI(input, 60));

    expected = new TestData();
    expected.add("0 f q TX_DONE 17", "");
    expected.add("0 f q WRITE 52", "48");
    expected.add("0 f q WRITE 20", "17 PRIMARY");
    expected.add("0 f q DATA 48", "18");

    Assert.assertEquals(expected, output);
  }

  @Test
  public void testLock() {
    TestData input = new TestData();

    input.add("0 f q WRITE 16", "11");
    input.add("0 f q LOCK 21", "1 f q");
    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q DATA 21", "18");
    input.add("0 f q DATA 11", "15");

    TestData output = new TestData(newGCI(input, 10));

    TestData expected = new TestData();
    expected.add("0 f q WRITE 16", "11");
    expected.add("0 f q LOCK 21", "1 f q");
    expected.add("0 f q DATA 21", "18");
    expected.add("0 f q DATA 11", "15");

    Assert.assertEquals(expected, output);
  }

  @Test
  public void testDelLockPartialCompaction() {
    // on partial compaction can delete data that del lock points to... but should keep del lock
    // until full major compaction

    TestData input = new TestData();

    input.add("0 f q DEL_LOCK 19", "0 ROLLBACK");
    input.add("0 f q LOCK 19", "1 f q");
    input.add("0 f q DEL_LOCK 11", "0 ROLLBACK");
    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q DATA 19", "19");
    input.add("0 f q DATA 11", "15");

    TestData output = new TestData(newGCI(input, 23, false));

    TestData expected = new TestData();
    expected.add("0 f q DEL_LOCK 19", "0 ROLLBACK");
    expected.add("0 f q DEL_LOCK 11", "0 ROLLBACK");

    Assert.assertEquals(expected, output);
  }

  @Test
  public void testDelLock() {
    TestData input = new TestData();

    input.add("0 f q DEL_LOCK 19", "0 ROLLBACK");
    input.add("0 f q LOCK 19", "1 f q");
    input.add("0 f q DEL_LOCK 11", "0 ROLLBACK");
    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q DATA 19", "19");
    input.add("0 f q DATA 11", "15");

    TestData output = new TestData(newGCI(input, 23));

    TestData expected = new TestData();

    Assert.assertEquals(expected, output);

    // test write that supercedes a del lock
    input = new TestData();
    input.add("0 f q WRITE 22", "21");
    input.add("0 f q DEL_LOCK 19", "0 ROLLBACK");
    input.add("0 f q LOCK 19", "1 f q");
    input.add("0 f q DEL_LOCK 11", "0 ROLLBACK");
    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q DATA 21", "17");
    input.add("0 f q DATA 19", "19");
    input.add("0 f q DATA 11", "15");

    output = new TestData(newGCI(input, 23));

    expected = new TestData();
    expected.add("0 f q WRITE 22", "21");
    expected.add("0 f q DATA 21", "17");

    Assert.assertEquals(expected, output);

    // test del_lock followed by write...
    input = new TestData();
    input.add("0 f q DEL_LOCK 19", "0 ROLLBACK");
    input.add("0 f q LOCK 19", "1 f q");
    input.add("0 f q WRITE 15", "11");
    input.add("0 f q DATA 19", "19");
    input.add("0 f q DATA 11", "15");

    output = new TestData(newGCI(input, 23));

    expected = new TestData();
    expected.add("0 f q WRITE 15", "11");
    expected.add("0 f q DATA 11", "15");

    Assert.assertEquals(expected, output);

    // test del_lock that is primary
    input = new TestData();
    input.add("0 f q WRITE 22", "19");
    input.add("0 f q DEL_LOCK 11", "13 PRIMARY");
    input.add("0 f q LOCK 11", "0 f q");
    input.add("0 f q DATA 19", "19");
    input.add("0 f q DATA 11", "15");

    output = new TestData(newGCI(input, 23));

    expected = new TestData();
    expected.add("0 f q WRITE 22", "19");
    expected.add("0 f q DEL_LOCK 11", "13 PRIMARY");
    expected.add("0 f q DATA 19", "19");

    Assert.assertEquals(expected, output);

    // add a TX_DONE.. should cause del lock to drop
    input.add("0 f q TX_DONE 13", "");

    output = new TestData(newGCI(input, 23));

    expected = new TestData();
    expected.add("0 f q WRITE 22", "19");
    expected.add("0 f q DATA 19", "19");

    Assert.assertEquals(expected, output);

    // ensure that timestamp in value is used
    input = new TestData();
    input.add("0 f q DEL_LOCK 3", "13 PRIMARY");
    input.add("0 f q LOCK 11", "0 f q");
    input.add("0 f q DATA 11", "15");

    output = new TestData(newGCI(input, 23));

    Assert.assertEquals(input, output);

    // ensure timestamp in value is used test 2
    input = new TestData();

    input.add("0 f q WRITE 11", "10");
    input.add("0 f q DEL_LOCK 3", "13");
    input.add("0 f q LOCK 3", "0 f q");
    input.add("0 f q DATA 10", "17");
    input.add("0 f q DATA 3", "15");

    output = new TestData(newGCI(input, 23));

    expected = new TestData();
    expected.add("0 f q WRITE 11", "10");
    expected.add("0 f q DATA 10", "17");

    Assert.assertEquals(expected, output);
  }

  @Test
  public void testDeletes() {
    TestData inputBase = new TestData();

    inputBase.add("0 f q WRITE 52", "48 DELETE");
    inputBase.add("0 f q WRITE 45", "41");
    inputBase.add("0 f q LOCK 41", "0 f q");
    inputBase.add("0 f q WRITE 38", "33");
    inputBase.add("0 f q LOCK 33", "0 f q");
    inputBase.add("0 f q DATA 41", "15");
    inputBase.add("0 f q DATA 33", "13");
    inputBase.add("0 g q WRITE 42", "38 DELETE");
    inputBase.add("0 g q WRITE 35", "31");
    inputBase.add("0 g q WRITE 28", "23");
    inputBase.add("0 g q DATA 31", "15");
    inputBase.add("0 g q DATA 23", "13");
    inputBase.add("0 h q WRITE 37", "33");
    inputBase.add("0 h q DATA 33", "9");

    TestData output = new TestData(newGCI(inputBase, 60));
    TestData expected = new TestData();
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);

    output = new TestData(newGCI(inputBase, 60, false));
    expected = new TestData();
    expected.add("0 g q WRITE 42", "38 DELETE");
    expected.add("0 f q WRITE 52", "48 DELETE");
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);

    output = new TestData(newGCI(output, 60));
    expected = new TestData();
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);

    output = new TestData(newGCI(inputBase, 37));
    expected = new TestData();
    expected.add("0 f q WRITE 52", "48 DELETE");
    expected.add("0 f q WRITE 45", "41");
    expected.add("0 f q WRITE 38", "33");
    expected.add("0 f q DATA 41", "15");
    expected.add("0 f q DATA 33", "13");
    expected.add("0 g q WRITE 42", "38 DELETE");
    expected.add("0 g q WRITE 35", "31");
    expected.add("0 g q DATA 31", "15");
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);

    output = new TestData(newGCI(inputBase, 43));
    expected = new TestData();
    expected.add("0 f q WRITE 52", "48 DELETE");
    expected.add("0 f q WRITE 45", "41");
    expected.add("0 f q WRITE 38", "33");
    expected.add("0 f q DATA 41", "15");
    expected.add("0 f q DATA 33", "13");
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);

    // test write that supercededs a delete... delete should not cause write to be GCed
    TestData input = new TestData(inputBase);
    input.add("0 f q WRITE 60", "55");
    input.add("0 f q DATA 55", "abc");
    output = new TestData(newGCI(input, 70));
    expected = new TestData();
    expected.add("0 f q WRITE 60", "55");
    expected.add("0 f q DATA 55", "abc");
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);

    // test lock after delete write
    input = new TestData(inputBase);
    input.add("0 g q LOCK 50", "0 g q");
    output = new TestData(newGCI(input, 70));
    expected = new TestData();
    expected.add("0 g q LOCK 50", "0 g q");
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);

    // test ack before delete write
    input = new TestData(inputBase);
    input.add("0 g q ACK 37", "");
    output = new TestData(newGCI(input, 70));
    expected = new TestData();
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);

    // test ack after delete write
    input = new TestData(inputBase);
    input.add("0 g q ACK 45", "");
    output = new TestData(newGCI(input, 70));
    expected = new TestData();
    expected.add("0 g q ACK 45", "");
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testDeletePrimary() {
    TestData inputBase = new TestData();

    inputBase.add("0 f q WRITE 52", "48 DELETE PRIMARY");
    inputBase.add("0 f q WRITE 45", "41");
    inputBase.add("0 f q WRITE 38", "33");
    inputBase.add("0 f q DATA 41", "15");
    inputBase.add("0 f q DATA 33", "13");
    inputBase.add("0 g q WRITE 42", "38 DELETE");
    inputBase.add("0 g q WRITE 35", "31");
    inputBase.add("0 g q WRITE 28", "23");
    inputBase.add("0 g q DATA 31", "15");
    inputBase.add("0 g q DATA 23", "13");
    inputBase.add("0 h q WRITE 37", "33");
    inputBase.add("0 h q DATA 33", "9");

    // ensure delete is not dropped if its primary (unless there is a TX DONE marker)
    TestData output = new TestData(newGCI(inputBase, 60));
    TestData expected = new TestData();
    expected.add("0 f q WRITE 52", "48 DELETE PRIMARY");
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);

    TestData input = new TestData(inputBase);
    input.add("0 f q TX_DONE 52", "");
    output = new TestData(newGCI(input, 60));
    expected = new TestData();
    expected.add("0 h q WRITE 37", "33");
    expected.add("0 h q DATA 33", "9");
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testNotifications() {
    // should pass notifications through
    TestData input = new TestData();

    input.add("0 ntfy foo:bar 7", "");
    input.add("0 ntfy foo:bar 5", "");
    input.add("0 ntfy foo:bar 4", "");

    input.add("1 ntfy foo:bar 3", "");
    input.add("1 ntfy foo:bar 2", "");

    input.add("1 ntfy foo:baz 1", "");
    input.add("2 ntfy foo:baz 3", "");

    TestData output = new TestData(newGCI(input, 60, false));
    Assert.assertEquals(input, output);
  }

  @Test
  public void testMultiColumn() {

    TestData input = new TestData();

    // important that del lock has same timestamp as lock in another column... should not delete the
    // lock or data in other column
    input.add("0 f a DEL_LOCK 19", "0 ROLLBACK");
    input.add("0 f a LOCK 19", "1 f q");
    input.add("0 f a DATA 19", "15");

    input.add("0 f b LOCK 19", "1 f q");
    input.add("0 f b DATA 19", "16");

    input.add("0 f c TX_DONE 20", "");

    input.add("0 f d WRITE 25", "21");
    input.add("0 f d WRITE 20", "17 PRIMARY");
    input.add("0 f d DATA 21", "19");
    input.add("0 f d DATA 17", "15");

    input.add("0 f e LOCK 19", "1 f q");
    input.add("0 f e DATA 19", "16");

    TestData output = new TestData(newGCI(input, 27));

    TestData expected = new TestData();

    expected.add("0 f b LOCK 19", "1 f q");
    expected.add("0 f b DATA 19", "16");

    expected.add("0 f c TX_DONE 20", "");

    expected.add("0 f d WRITE 25", "21");
    expected.add("0 f d WRITE 20", "17 PRIMARY");
    expected.add("0 f d DATA 21", "19");

    expected.add("0 f e LOCK 19", "1 f q");
    expected.add("0 f e DATA 19", "16");

    Assert.assertEquals(expected, output);
  }
}
