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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.junit.Assert;
import org.junit.Test;

public class BytesBuilderTest {
  @Test
  public void testBasic() {
    BytesBuilder bb = Bytes.builder();

    Bytes bytes1 = bb.append(new byte[] {'a', 'b'}).append("cd").append(Bytes.of("ef")).toBytes();
    Assert.assertEquals(Bytes.of("abcdef"), bytes1);

    bb = Bytes.builder();
    Bytes bytes2 = bb.append(Bytes.of("ab")).append("cd").append(new byte[] {'e', 'f'}).toBytes();
    Assert.assertEquals(Bytes.of("abcdef"), bytes2);
  }

  @Test
  public void testInputStream() throws IOException {

    ByteArrayInputStream bais =
        new ByteArrayInputStream("abcdefg".getBytes(StandardCharsets.UTF_8));
    BytesBuilder bb = Bytes.builder();

    bb.append(bais, 2);
    bb.append(bais, 3);

    Assert.assertEquals(Bytes.of("abcde"), bb.toBytes());
  }

  @Test
  public void testByteBuffer() {
    ByteBuffer buffer = ByteBuffer.wrap("abcdefg".getBytes(StandardCharsets.UTF_8));

    BytesBuilder bb = Bytes.builder();

    bb.append(buffer);
    bb.append(buffer);

    Assert.assertEquals(0, buffer.position());
    Assert.assertEquals(7, buffer.remaining());

    Assert.assertEquals(Bytes.of("abcdefg" + "abcdefg"), bb.toBytes());
  }

  @Test
  public void testSetLength() {
    BytesBuilder bb = Bytes.builder();

    Bytes bytes1 = bb.append(new byte[] {'a', 'b'}).append("cd").append(Bytes.of("ef")).toBytes();
    Assert.assertEquals(Bytes.of("abcdef"), bytes1);

    bb.setLength(0);
    Bytes bytes2 = bb.append(Bytes.of("ab")).append("cd").append(new byte[] {'e', 'f'}).toBytes();
    Assert.assertEquals(Bytes.of("abcdef"), bytes2);

    bb.setLength(10);
    Bytes bytes3 = bb.toBytes();
    Assert.assertEquals(Bytes.of(new byte[] {'a', 'b', 'c', 'd', 'e', 'f', 0, 0, 0, 0}), bytes3);

    bb.setLength(100);
    Bytes bytes4 = bb.toBytes();
    Assert.assertEquals(Bytes.of("abcdef"), bytes4.subSequence(0, 6));
    for (int i = 6; i < 100; i++) {
      Assert.assertEquals(0, bytes4.byteAt(i));
    }
  }

  @Test
  public void testSingleByte() {
    BytesBuilder bb = Bytes.builder();

    bb.append('c');
    bb.append(0);
    bb.append(127);
    bb.append(128);
    bb.append(255);

    Bytes bytes = bb.toBytes();
    Assert.assertEquals(Bytes.of(new byte[] {'c', 0, 127, (byte) 128, (byte) 0xff}), bytes);
  }

  @Test
  public void testArraySection() {
    BytesBuilder bb = Bytes.builder();

    byte[] testing = new byte[] {'a', 'b', 'c', 'd', 'e'};

    bb.append(testing, 0, 3);
    bb.append(testing, 1, 3);
    bb.append(testing, 2, 2);

    Bytes bytes = bb.toBytes();
    Assert.assertEquals(Bytes.of("abcbcdcd"), bytes);
  }

  @Test
  public void testIncreaseCapacity() {

    // test appending 3 chars at a time
    StringBuilder sb = new StringBuilder();
    BytesBuilder bb = Bytes.builder();
    BytesBuilder bb2 = Bytes.builder();
    BytesBuilder bb3 = Bytes.builder();
    int m = 19;
    for (int i = 0; i < 100; i++) {
      // produce a deterministic non repeating pattern
      String s = (m % 1000) + "";
      m = Math.abs(m * 19 + i);

      bb.append(s);
      bb2.append(Bytes.of(s));
      bb3.append(s);
      sb.append(s);
    }

    Assert.assertEquals(Bytes.of(sb.toString()), bb.toBytes());
    Assert.assertEquals(Bytes.of(sb.toString()), bb2.toBytes());
    Assert.assertEquals(Bytes.of(sb.toString()), bb3.toBytes());

    // test appending one char at a time
    sb.setLength(0);
    bb = Bytes.builder();
    bb2 = Bytes.builder();
    bb3.setLength(0);
    for (int i = 0; i < 500; i++) {
      // produce a deterministic non repeating pattern
      String s = (m % 10) + "";
      m = Math.abs(m * 19 + i);
      sb.append(s);
      bb.append(s);
      bb2.append(Bytes.of(s));
      bb3.append(s);

    }

    Assert.assertEquals(Bytes.of(sb.toString()), bb.toBytes());
    Assert.assertEquals(Bytes.of(sb.toString()), bb2.toBytes());
    Assert.assertEquals(Bytes.of(sb.toString()), bb3.toBytes());
  }

  @Test
  public void testCharSequence() {
    AsciiSequence cs1 = new AsciiSequence("abc123");
    AsciiSequence cs2 = new AsciiSequence("xyz789");


    Bytes b3 = Bytes.builder().append(cs1).append(":").append(cs2).toBytes();

    Assert.assertEquals("abc123:xyz789", b3.toString());

    Bytes b4 = Bytes.builder().append((CharSequence) "abc123").append(":")
        .append((CharSequence) "xyz789").toBytes();

    Assert.assertEquals("abc123:xyz789", b4.toString());
  }
}
