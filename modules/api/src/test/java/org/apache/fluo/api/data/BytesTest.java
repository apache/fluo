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

package org.apache.fluo.api.data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

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
  public void testImmutable() {
    byte[] d1 = Bytes.of("mydata").toArray();

    Bytes imm = Bytes.of(d1);
    Assert.assertNotSame(d1, imm.toArray());
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

  @Test
  public void testToByteBuffer() {
    Bytes b1 = Bytes.of("fluofluo");
    ByteBuffer buffer = b1.toByteBuffer();
    Assert.assertFalse(buffer.hasArray());
    Assert.assertEquals(buffer.remaining(), 8);

    byte[] copy = new byte[8];
    buffer.duplicate().get(copy);
    Assert.assertEquals("fluofluo", new String(copy, StandardCharsets.UTF_8));

    try {
      buffer.put((byte) 6);
      Assert.fail();
    } catch (ReadOnlyBufferException e) {
    }
  }

  @Test
  public void testWrite() throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();

    byte[] ba1 = "abc".getBytes(StandardCharsets.UTF_8);
    Bytes little = Bytes.of(ba1);
    byte[] ba2 = new byte[1024];
    Arrays.fill(ba2, (byte) 42);
    Bytes big = Bytes.of(ba2);

    little.writeTo(out);
    big.writeTo(out);

    byte[] expected = new byte[ba1.length + ba2.length];
    System.arraycopy(ba1, 0, expected, 0, ba1.length);
    System.arraycopy(ba2, 0, expected, ba1.length, ba2.length);

    Assert.assertArrayEquals(expected, out.toByteArray());
    out.close();
  }

  @Test
  public void testBounds() {
    Bytes bytes = Bytes.of("abcdefg").subSequence(2, 5);
    Assert.assertEquals("abcdefg".substring(2, 5), bytes.toString());
    Assert.assertEquals('c', bytes.byteAt(0));
    Assert.assertEquals('d', bytes.byteAt(1));
    Assert.assertEquals('e', bytes.byteAt(2));

    Assert.assertEquals("de", bytes.subSequence(1, 3).toString());

    try {
      bytes.byteAt(-1);
      Assert.fail();
    } catch (IndexOutOfBoundsException e) {
    }

    try {
      bytes.byteAt(3);
      Assert.fail();
    } catch (IndexOutOfBoundsException e) {
    }

    try {
      bytes.subSequence(-1, 2);
      Assert.fail();
    } catch (IndexOutOfBoundsException e) {
    }

    try {
      bytes.subSequence(0, 5);
      Assert.fail();
    } catch (IndexOutOfBoundsException e) {
    }

    try {
      bytes.subSequence(2, 1);
      Assert.fail();
    } catch (IndexOutOfBoundsException e) {
    }
  }
}
