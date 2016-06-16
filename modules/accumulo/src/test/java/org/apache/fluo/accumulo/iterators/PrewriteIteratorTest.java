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

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedMapIterator;
import org.junit.Assert;
import org.junit.Test;

public class PrewriteIteratorTest {

  PrewriteIterator newPI(TestData input, long snapTime) {
    PrewriteIterator ni = new PrewriteIterator();

    IteratorEnvironment env = TestIteratorEnv.create(IteratorScope.scan, false);

    try {
      IteratorSetting cfg = new IteratorSetting(10, PrewriteIterator.class);
      PrewriteIterator.setSnaptime(cfg, snapTime);
      ni.init(new SortedMapIterator(input.data), cfg.getOptions(), env);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ni;
  }

  PrewriteIterator newPI(TestData input, long snapTime, long ntfyTime) {
    PrewriteIterator ni = new PrewriteIterator();

    IteratorEnvironment env = TestIteratorEnv.create(IteratorScope.scan, false);

    try {
      IteratorSetting cfg = new IteratorSetting(10, PrewriteIterator.class);
      PrewriteIterator.setSnaptime(cfg, snapTime);
      PrewriteIterator.enableAckCheck(cfg, ntfyTime);
      ni.init(new SortedMapIterator(input.data), cfg.getOptions(), env);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ni;
  }

  public void addLots(TestData input) {
    for (int i = 3; i < 100; i += 3) {
      input.add("0 f q TX_DONE " + (i + 1), "" + i);
      input.add("0 f q WRITE " + (i + 1), "" + i);
      input.add("0 f q LOCK " + i, "0 f q");
      input.add("0 f q DATA " + i, "15");
    }
  }

  @Test
  public void testAck() {
    TestData input = new TestData();

    input.add("0 f q WRITE 116", "111");
    input.add("0 f q LOCK 111", "0 f q");
    input.add("0 f q DATA 111", "15");
    input.add("0 f q ACK 111", "");

    for (int i = 0; i < 2; i++) {
      TestData output = new TestData(newPI(input, 117, 100), Range.exact("0", "f", "q"));
      TestData expected = new TestData().add("0 f q ACK 111", "");
      Assert.assertEquals(expected, output);

      output = new TestData(newPI(input, 117, 116), Range.exact("0", "f", "q"));
      Assert.assertEquals(0, output.data.size());

      output = new TestData(newPI(input, 112, 100), Range.exact("0", "f", "q"));
      expected = new TestData().add("0 f q WRITE 116", "111");
      Assert.assertEquals(expected, output);

      addLots(input);
      input.add("0 f q TX_DONE 116", "111");
      for (int j = 3; j < 100; j += 3) {
        input.add("0 f q ACK " + j, "");
      }
    }
  }

  @Test
  public void testLockedCell() throws Exception {
    TestData input = new TestData();

    // the row is locked
    input.add("0 f q WRITE 116", "111");
    input.add("0 f q LOCK 121", "0 f q");
    input.add("0 f q LOCK 111", "0 f q");
    input.add("0 f q DATA 111", "15");

    TestData expected = new TestData().add("0 f q LOCK 121", "0 f q");

    for (long ts : new long[] {117, 122}) {
      TestData output = new TestData(newPI(input, ts), Range.exact("0", "f", "q"));
      Assert.assertEquals(expected, output);
    }

    input.add("0 f q TX_DONE 116", "111");
    for (long ts : new long[] {117, 122}) {
      TestData output = new TestData(newPI(input, ts), Range.exact("0", "f", "q"));
      Assert.assertEquals(expected, output);
    }

    addLots(input);
    for (long ts : new long[] {117, 122}) {
      TestData output = new TestData(newPI(input, ts), Range.exact("0", "f", "q"));
      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void testWriteAfterStart() throws Exception {
    TestData input = new TestData();

    // the row is locked
    input.add("0 f q WRITE 116", "111");
    input.add("0 f q LOCK 121", "0 f q");
    input.add("0 f q LOCK 111", "0 f q");
    input.add("0 f q DATA 111", "15");

    TestData expected = new TestData().add("0 f q WRITE 116", "111");

    for (long ts : new long[] {108, 112}) {
      TestData output = new TestData(newPI(input, ts), Range.exact("0", "f", "q"));
      Assert.assertEquals(expected, output);
    }

    input.add("0 f q TX_DONE 116", "111");
    for (long ts : new long[] {108, 112}) {
      TestData output = new TestData(newPI(input, ts), Range.exact("0", "f", "q"));
      Assert.assertEquals(expected, output);
    }

    addLots(input);
    for (long ts : new long[] {108, 112}) {
      TestData output = new TestData(newPI(input, ts), Range.exact("0", "f", "q"));
      Assert.assertEquals(expected, output);
    }
  }

  @Test
  public void testDelLock() {
    TestData input = new TestData();

    // the row is locked
    input.add("0 f q DEL_LOCK 111", "ABORT");
    input.add("0 f q LOCK 111", "0 f q");
    input.add("0 f q DATA 111", "15");

    for (int i = 0; i < 2; i++) {

      TestData output = new TestData(newPI(input, 117), Range.exact("0", "f", "q"));
      Assert.assertEquals(new TestData(), output);

      output = new TestData(newPI(input, 108), Range.exact("0", "f", "q"));
      TestData expected = new TestData();
      expected.add("0 f q DEL_LOCK 111", "ABORT");
      Assert.assertEquals(expected, output);

      input.add("0 f q TX_DONE 116", "111");
      output = new TestData(newPI(input, 117), Range.exact("0", "f", "q"));
      Assert.assertEquals(0, output.data.size());

      output = new TestData(newPI(input, 108), Range.exact("0", "f", "q"));
      Assert.assertEquals(expected, output);

      addLots(input);
    }
  }

  @Test
  public void testWriteAndDelLockMax() {
    // ensure the delete lock or write with max timestamp invalidates lock
    TestData input = new TestData();

    input.add("0 f q WRITE 116", "111");
    input.add("0 f q LOCK 111", "0 f q");
    input.add("0 f q DEL_LOCK 108", "ABORT");

    TestData output = new TestData(newPI(input, 117), Range.exact("0", "f", "q"));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newPI(input, 111), Range.exact("0", "f", "q"));
    TestData expected = new TestData();
    expected.add("0 f q WRITE 116", "111");
    Assert.assertEquals(expected, output);

    input = new TestData();

    input.add("0 f q DEL_LOCK 116", "ABORT");
    input.add("0 f q LOCK 111", "0 f q");
    input.add("0 f q WRITE 108", "100");

    output = new TestData(newPI(input, 117), Range.exact("0", "f", "q"));
    Assert.assertEquals(0, output.data.size());

    output = new TestData(newPI(input, 111), Range.exact("0", "f", "q"));
    expected = new TestData();
    expected.add("0 f q DEL_LOCK 116", "ABORT");
    Assert.assertEquals(expected, output);
  }

  @Test
  public void testWriteInvalidatesLock() {
    TestData input = new TestData();

    // the row is locked
    input.add("0 f q WRITE 116", "111");
    input.add("0 f q LOCK 111", "0 f q");
    input.add("0 f q DATA 111", "15");

    TestData output = new TestData(newPI(input, 117), Range.exact("0", "f", "q"));
    Assert.assertEquals(0, output.data.size());

    input.add("0 f q TX_DONE 116", "111");
    output = new TestData(newPI(input, 117), Range.exact("0", "f", "q"));
    Assert.assertEquals(0, output.data.size());

    addLots(input);
    output = new TestData(newPI(input, 117), Range.exact("0", "f", "q"));
    Assert.assertEquals(0, output.data.size());
  }
}
