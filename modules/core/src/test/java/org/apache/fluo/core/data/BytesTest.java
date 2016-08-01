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

package org.apache.fluo.core.data;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.fluo.api.data.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link Bytes}
 */
public class BytesTest {

  @Test
  public void testBytesWrap() {

    String s1 = "test1";
    Bytes b1 = Bytes.of(s1);
    Assert.assertArrayEquals(s1.getBytes(), b1.toArray());
    Assert.assertEquals(s1, b1.toString());

    String s2 = "test2";
    ByteBuffer bb = ByteBuffer.wrap(s2.getBytes());
    Bytes b2 = Bytes.of(bb);
    Assert.assertArrayEquals(s2.getBytes(), b2.toArray());
    Assert.assertEquals(s2, b2.toString());

    // call again to ensure that position was not changed by previous call
    b2 = Bytes.of(bb);
    Assert.assertArrayEquals(s2.getBytes(), b2.toArray());
    Assert.assertEquals(s2, b2.toString());

    String s3 = "test3";
    Bytes b3 = Bytes.of(s3.getBytes());
    Assert.assertArrayEquals(s3.getBytes(), b3.toArray());
    Assert.assertEquals(s3, b3.toString());

    String s4 = "test4";
    byte[] d4 = s4.getBytes();
    Bytes b4 = Bytes.of(d4, 0, d4.length);
    Assert.assertArrayEquals(s4.getBytes(), b4.toArray());
    Assert.assertEquals(s4, b4.toString());
  }

  @Test
  public void testConcatSplit() {

    Bytes b1 = Bytes.of("str1");
    Bytes b2 = Bytes.of("string2");
    Bytes b3 = Bytes.of("s3");
    Bytes ball = Bytes.concat(b1, b2, b3);

    List<Bytes> blist = Bytes.split(ball);

    Assert.assertEquals(b1, blist.get(0));
    Assert.assertEquals(b2, blist.get(1));
    Assert.assertEquals(b3, blist.get(2));
  }

  @Test
  public void testImmutable() {
    byte[] d1 = Bytes.of("mydata").toArray();

    Bytes imm = Bytes.of(d1);
    Assert.assertNotSame(d1, imm.toArray());
  }

  @Test
  public void testHashSubsequence() {
    Bytes b1 = Bytes.of("abcde");
    Bytes b2 = Bytes.of("cde");

    Assert.assertEquals(b2.hashCode(), b1.subSequence(2, 5).hashCode());
  }

  @Test
  public void testCompare() {
    Bytes b1 = Bytes.of("a");
    Bytes b2 = Bytes.of("b");
    Bytes b3 = Bytes.of("a");
    Assert.assertEquals(-1, b1.compareTo(b2));
    Assert.assertEquals(1, b2.compareTo(b1));
    Assert.assertEquals(0, b1.compareTo(b3));
    Assert.assertEquals(1, b1.compareTo(Bytes.EMPTY));
  }
}
