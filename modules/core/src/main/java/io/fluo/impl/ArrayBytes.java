/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.impl;

import io.fluo.api.Bytes;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.io.Text;

/**
* An implementation of {@link Bytes} that uses a backing byte array.
*/
public class ArrayBytes extends Bytes implements Serializable {

  private static final long serialVersionUID = 1L;

  protected byte data[];
  protected int offset;
  protected int length;

  /**
   * Creates a new ArrayBytes. The given byte array is used directly as the
   * backing array, so later changes made to the array reflect into the new
   * sequence.
   *
   * @param data byte data
   */
  public ArrayBytes(byte data[]) {
    this.data = data;
    this.offset = 0;
    this.length = data.length;
  }

  /**
   * Creates a new ArrayBytes from a subsequence of the given byte array. The
   * given byte array is used directly as the backing array, so later changes
   * made to the (relevant portion of the) array reflect into the new sequence.
   *
   * @param data byte data
   * @param offset starting offset in byte array (inclusive)
   * @param length number of bytes to include in sequence
   * @throws IllegalArgumentException if the offset or length are out of bounds
   * for the given byte array
   */
  public ArrayBytes(byte data[], int offset, int length) {
    if (offset < 0 || offset > data.length || length < 0 || (offset + length) > data.length) {
      throw new IllegalArgumentException(" Bad offset and/or length data.length = " + data.length + " offset = " + offset + " length = " + length);
    }
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Creates a new ArrayBytes from the given string. The bytes are determined from
   * the string using the default platform encoding.
   *
   * @param s String to represent as Bytes
   */
  public ArrayBytes(String s) {
    this(s.getBytes(StandardCharsets.UTF_8));
  }
  
  /**
   * Creates a new ArrayBytes from the given string. The bytes are determined from
   * the string using the default platform encoding.
   *
   * @param s String to represent as Bytes
   */
  public ArrayBytes(String s, Charset cs) {
    this(s.getBytes(cs));
  }

  /**
   * Creates a new ArrayBytes based on a ByteBuffer. If the byte buffer has an
   * array, that array (and the buffer's offset and limit) are used; otherwise,
   * a new backing array is created and a relative bulk get is performed to
   * transfer the buffer's contents (starting at its current position and
   * not beyond its limit).
   *
   * @param buffer byte buffer
   */
  public ArrayBytes(ByteBuffer buffer) {
    this.length = buffer.remaining();

    if (buffer.hasArray()) {
      this.data = buffer.array();
      this.offset = buffer.position();
    } else {
      this.data = new byte[length];
      this.offset = 0;
      buffer.get(data);
    }
  }
  
  /**
   * Creates a new ArrayBytes based on a ByteSequence. If the byte buffer has an
   * array, that array (and the buffer's offset and limit) are used; otherwise,
   * a new backing array is created and a relative bulk get is performed to
   * transfer the buffer's contents (starting at its current position and
   * not beyond its limit).
   *
   * @param bs ByteSequence
   */
  public ArrayBytes(ByteSequence bs) {
    this.length = bs.length();

    if (bs.isBackedByArray()) {
      this.data = bs.getBackingArray();
      this.offset = bs.offset();
    } else {
      this.data = bs.toArray();
      this.offset = 0;
    }
  }

  @Override
  public byte byteAt(int i) {

    if (i < 0) {
      throw new IllegalArgumentException("i < 0, " + i);
    }

    if (i >= length) {
      throw new IllegalArgumentException("i >= length, " + i + " >= " + length);
    }

    return data[offset + i];
  }

  @Override
  public byte[] getBackingArray() {
    return data;
  }

  @Override
  public boolean isBackedByArray() {
    return true;
  }

  @Override
  public int length() {
    return length;
  }

  @Override
  public int offset() {
    return offset;
  }

  @Override
  public Bytes subSequence(int start, int end) {

    if (start > end || start < 0 || end > length) {
      throw new IllegalArgumentException("Bad start and/end start = " + start + " end=" + end + " offset=" + offset + " length=" + length);
    }

    return new ArrayBytes(data, offset + start, end - start);
  }

  @Override
  public byte[] toArray() {
    if (offset == 0 && length == data.length)
      return data;

    byte[] copy = new byte[length];
    System.arraycopy(data, offset, copy, 0, length);
    return copy;
  }

  /** 
   * Creates UTF-8 String using Bytes data
   */
  public String toString() {
    return new String(data, offset, length, StandardCharsets.UTF_8);
  }

  /**
   * Creates ByteSequence using Bytes data
   * 
   * @return ByteSequence
   */
  public ByteSequence toByteSequence() {
    return new ArrayByteSequence(data, offset, length);
  }

  /**
   * Creates Hadoop Text object using Bytes Data
   * 
   * @return Hadoop Text object 
   */
  public Text toText() {
    Text t = new Text();
    t.set(data, offset, length);
    return t;
  }
}
