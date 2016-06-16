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

package org.apache.fluo.accumulo.data;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import org.apache.fluo.api.data.Bytes;

/**
 * An implementation of {@link Bytes} that is mutable and uses a backing byte array
 */
public class MutableBytes extends Bytes implements Serializable {

  private static final long serialVersionUID = 1L;

  private final byte[] data;
  private final int offset;
  private final int length;

  public MutableBytes() {
    this.data = null;
    this.offset = 0;
    this.length = 0;
  }

  /**
   * Creates a new MutableBytes. The given byte array is used directly as the backing array so later
   * changes made to the array reflect into the new sequence.
   */
  public MutableBytes(byte[] data) {
    this.data = data;
    this.offset = 0;
    this.length = data.length;
  }

  /**
   * Creates a new MutableBytes from a subsequence of the given byte array. The given byte array is
   * used directly as the backing array, so later changes made to the (relevant portion of the)
   * array reflect into the new sequence.
   *
   * @param data byte data
   * @param offset starting offset in byte array (inclusive)
   * @param length number of bytes to include in sequence
   * @throws IllegalArgumentException if the offset or length are out of bounds for the given byte
   *         array
   */
  public MutableBytes(byte[] data, int offset, int length) {
    if (offset < 0 || offset > data.length || length < 0 || (offset + length) > data.length) {
      throw new IllegalArgumentException(" Bad offset and/or length data.length = " + data.length
          + " offset = " + offset + " length = " + length);
    }
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Creates a new MutableBytes from the given string. The bytes are determined from the string
   * using UTF-8 encoding
   *
   * @param s String to represent as Bytes
   */
  public MutableBytes(String s) {
    this(s.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Creates a new MutableBytes from the given string. The bytes are determined from the string
   * using the specified charset
   *
   * @param s String to represent as Bytes
   * @param cs Charset
   */
  public MutableBytes(String s, Charset cs) {
    this(s.getBytes(cs));
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
  public int length() {
    return length;
  }

  @Override
  public Bytes subSequence(int start, int end) {
    if (start > end || start < 0 || end > length) {
      throw new IllegalArgumentException("Bad start and/end start = " + start + " end=" + end
          + " offset=" + offset + " length=" + length);
    }
    return new MutableBytes(data, offset + start, end - start);
  }

  @Override
  public byte[] toArray() {
    byte[] copy = new byte[length];
    System.arraycopy(data, offset, copy, 0, length);
    return copy;
  }

  /**
   * Creates UTF-8 String using Bytes data
   */
  @Override
  public String toString() {
    return new String(data, offset, length, StandardCharsets.UTF_8);
  }
}
