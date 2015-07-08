/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.accumulo.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

public class GarbageCollectionIteratorTest {

  GarbageCollectionIterator newGCI(TestData input, long oldestActive) {
    GarbageCollectionIterator gci = new GarbageCollectionIterator();
    Map<String, String> options = new HashMap<>();
    options.put(GarbageCollectionIterator.OLDEST_ACTIVE_TS_OPT, Long.toString(oldestActive));
    TestIteratorEnv env = new TestIteratorEnv(IteratorScope.majc);

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
    expected.add("0 f q WRITE 16", "11 TRUNCATION");
    expected.add("0 f q DATA 11", "15");

    Assert.assertEquals(expected, output);
  }

  @Test
  public void testAckRemoval() {

    TestData input = new TestData();
    // TODO need to test w/ more types
    input.add("0 f q TX_DONE 21", "");
    input.add("0 f q WRITE 21", "20 TRUNCATION PRIMARY");
    input.add("0 f q ACK 20", "");
    input.add("0 f q ACK 16", "");
    input.add("0 f q ACK 14", "");

    TestData expected = new TestData();
    expected.add("0 f q TX_DONE 21", "");
    expected.add("0 f q WRITE 21", "20 TRUNCATION PRIMARY");

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
    input.add("0 f q WRITE 19", "16 TRUNCATION PRIMARY");
    input.add("0 f q ACK 20", "");
    input.add("0 f q ACK 16", "");
    input.add("0 f q ACK 14", "");

    expected = new TestData();
    expected.add("0 f q WRITE 19", "16 TRUNCATION PRIMARY");
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
    input.add("0 f q WRITE 20", "17 TRUNCATION");
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
    expected.add("0 f q WRITE 38", "33 TRUNCATION");
    expected.add("0 f q DATA 48", "18");
    expected.add("0 f q DATA 41", "15");
    expected.add("0 f q DATA 33", "13");

    output = new TestData(newGCI(input, 39));
    Assert.assertEquals(6, expected.data.size());
    Assert.assertEquals(expected, output);

    expected = new TestData();
    expected.add("0 f q WRITE 52", "48");
    expected.add("0 f q WRITE 45", "41 TRUNCATION");
    expected.add("0 f q DATA 48", "18");
    expected.add("0 f q DATA 41", "15");

    output = new TestData(newGCI(input, 46));
    Assert.assertEquals(4, expected.data.size());
    Assert.assertEquals(expected, output);

    expected = new TestData();
    expected.add("0 f q WRITE 52", "48 TRUNCATION");
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
    input.add("0 f q WRITE 20", "17 TRUNCATION PRIMARY");

    TestData output = new TestData(newGCI(input, 60));

    TestData expected = new TestData();
    expected.add("0 f q WRITE 52", "48 TRUNCATION");
    expected.add("0 f q DATA 48", "18");

    Assert.assertEquals(expected, output);

    // test with only primary
    input = new TestData(inputBase);

    input.add("0 f q WRITE 20", "17 TRUNCATION PRIMARY");

    output = new TestData(newGCI(input, 60));

    expected = new TestData();
    expected.add("0 f q WRITE 52", "48 TRUNCATION");
    expected.add("0 f q WRITE 20", "17 TRUNCATION PRIMARY");
    expected.add("0 f q DATA 48", "18");

    Assert.assertEquals(expected, output);

    // test with only txdone
    input = new TestData(inputBase);

    input.add("0 f q TX_DONE 20", "");

    output = new TestData(newGCI(input, 60));

    expected = new TestData();
    expected.add("0 f q TX_DONE 20", "");
    expected.add("0 f q WRITE 52", "48 TRUNCATION");
    expected.add("0 f q DATA 48", "18");

    Assert.assertEquals(expected, output);

    // test TX_DONE and WRITE where times do not match
    input = new TestData(inputBase);

    input.add("0 f q TX_DONE 17", "");
    input.add("0 f q WRITE 20", "17 TRUNCATION PRIMARY");

    output = new TestData(newGCI(input, 60));

    expected = new TestData();
    expected.add("0 f q TX_DONE 17", "");
    expected.add("0 f q WRITE 52", "48 TRUNCATION");
    expected.add("0 f q WRITE 20", "17 TRUNCATION PRIMARY");
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
  public void testDelLock() {
    TestData input = new TestData();

    input.add("0 f q DEL_LOCK 19", "19");
    input.add("0 f q LOCK 19", "1 f q");
    input.add("0 f q DEL_LOCK 11", "11");
    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q DATA 19", "19");
    input.add("0 f q DATA 11", "15");

    TestData output = new TestData(newGCI(input, 23));

    TestData expected = new TestData();
    expected.add("0 f q DEL_LOCK 19", "19");
    expected.add("0 f q DATA 19", "19");
    expected.add("0 f q DATA 11", "15");

    Assert.assertEquals(expected, output);

    // test write that supercedes a del lock
    input = new TestData();
    input.add("0 f q WRITE 22", "21");
    input.add("0 f q DEL_LOCK 19", "19");
    input.add("0 f q LOCK 19", "1 f q");
    input.add("0 f q DEL_LOCK 11", "11");
    input.add("0 f q LOCK 11", "1 f q");
    input.add("0 f q DATA 21", "17");
    input.add("0 f q DATA 19", "19");
    input.add("0 f q DATA 11", "15");

    output = new TestData(newGCI(input, 23));

    expected = new TestData();
    expected.add("0 f q WRITE 22", "21 TRUNCATION");
    expected.add("0 f q DATA 21", "17");

    Assert.assertEquals(expected, output);

    // test del_lock followed by write... should keep del_lock
    input = new TestData();
    input.add("0 f q DEL_LOCK 19", "19");
    input.add("0 f q LOCK 19", "1 f q");
    input.add("0 f q WRITE 15", "11");
    input.add("0 f q DATA 19", "19");
    input.add("0 f q DATA 11", "15");

    output = new TestData(newGCI(input, 23));

    expected = new TestData();
    expected.add("0 f q DEL_LOCK 19", "19");
    expected.add("0 f q WRITE 15", "11 TRUNCATION");
    expected.add("0 f q DATA 19", "19");
    expected.add("0 f q DATA 11", "15");

    Assert.assertEquals(expected, output);

    // test del_lock that is primary
    input = new TestData();
    input.add("0 f q WRITE 22", "19");
    input.add("0 f q DEL_LOCK 13", "11 PRIMARY");
    input.add("0 f q LOCK 11", "0 f q");
    input.add("0 f q DATA 19", "19");
    input.add("0 f q DATA 11", "15");

    output = new TestData(newGCI(input, 23));

    expected = new TestData();
    expected.add("0 f q WRITE 22", "19 TRUNCATION");
    expected.add("0 f q DEL_LOCK 13", "11 PRIMARY");
    expected.add("0 f q DATA 19", "19");

    Assert.assertEquals(expected, output);

    // add a TX_DONE.. should cause del lock to drop
    input.add("0 f q TX_DONE 13", "");

    output = new TestData(newGCI(input, 23));

    expected = new TestData();
    expected.add("0 f q WRITE 22", "19 TRUNCATION");
    expected.add("0 f q DATA 19", "19");

    Assert.assertEquals(expected, output);

    // ensure that timestamp in value is used
    input = new TestData();
    input.add("0 f q DEL_LOCK 13", "3 PRIMARY");
    input.add("0 f q LOCK 11", "0 f q");
    input.add("0 f q DATA 11", "15");

    output = new TestData(newGCI(input, 23));

    Assert.assertEquals(input, output);


    // ensure timestamp in value is used test 2
    input = new TestData();

    input.add("0 f q WRITE 11", "10");
    input.add("0 f q DEL_LOCK 13", "3");
    input.add("0 f q LOCK 3", "0 f q");
    input.add("0 f q DATA 10", "17");
    input.add("0 f q DATA 3", "15");

    output = new TestData(newGCI(input, 23));

    expected = new TestData();
    expected.add("0 f q WRITE 11", "10 TRUNCATION");
    expected.add("0 f q DATA 10", "17");

    Assert.assertEquals(expected, output);
  }



  @Test
  public void testMultiColumn() {

    TestData input = new TestData();

    input.add("0 f a DEL_LOCK 19", "19");
    input.add("0 f a LOCK 19", "1 f q");
    input.add("0 f a DATA 19", "15");

    input.add("0 f b LOCK 19", "1 f q");
    input.add("0 f b DATA 19", "16");

    input.add("0 f c TX_DONE 20", "");

    input.add("0 f d WRITE 25", "21");
    input.add("0 f d WRITE 20", "17 TRUNCATION PRIMARY");
    input.add("0 f d DATA 21", "19");
    input.add("0 f d DATA 17", "15");

    input.add("0 f e LOCK 19", "1 f q");
    input.add("0 f e DATA 19", "16");

    TestData output = new TestData(newGCI(input, 27));

    TestData expected = new TestData();

    expected.add("0 f a DEL_LOCK 19", "19");
    expected.add("0 f a DATA 19", "15");

    expected.add("0 f b LOCK 19", "1 f q");
    expected.add("0 f b DATA 19", "16");

    expected.add("0 f c TX_DONE 20", "");

    expected.add("0 f d WRITE 25", "21 TRUNCATION");
    expected.add("0 f d WRITE 20", "17 TRUNCATION PRIMARY");
    expected.add("0 f d DATA 21", "19");

    expected.add("0 f e LOCK 19", "1 f q");
    expected.add("0 f e DATA 19", "16");

    Assert.assertEquals(expected, output);
  }
}
