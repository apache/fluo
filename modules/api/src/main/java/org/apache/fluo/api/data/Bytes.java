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
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents bytes in Fluo. Similar to an Accumulo ByteSequence. Bytes is immutable after it is
 * created. Bytes.EMPTY is used to represent a Bytes object with no data.
 *
 * @since 1.0.0
 */
public final class Bytes implements Comparable<Bytes>, Serializable {

  private static final long serialVersionUID = 1L;
  private static final String WRITE_UTIL_CLASS = "org.apache.fluo.accumulo.data.WriteUtilImpl";

  private final byte[] data;
  private final int offset;
  private final int length;

  /**
   * @since 1.0.0
   */
  public interface WriteUtil {
    void writeVInt(DataOutput stream, int i) throws IOException;

    int readVInt(DataInput stream) throws IOException;
  }

  private static WriteUtil writeUtil;

  static {
    try {
      writeUtil =
          (WriteUtil) Class.forName(WRITE_UTIL_CLASS).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static final Bytes EMPTY = new Bytes(new byte[0]);

  private Integer hashCode = null;

  private Bytes(byte[] data) {
    this.data = data;
    this.offset = 0;
    this.length = data.length;
  }

  private Bytes(byte[] data, int offset, int length) {
    if (offset < 0 || offset > data.length || length < 0 || (offset + length) > data.length) {
      throw new IndexOutOfBoundsException(" Bad offset and/or length data.length = " + data.length
          + " offset = " + offset + " length = " + length);
    }
    this.data = data;
    this.offset = offset;
    this.length = length;
  }

  /**
   * Gets a byte within this sequence of bytes
   *
   * @param i index into sequence
   * @return byte
   * @throws IllegalArgumentException if i is out of range
   */
  public byte byteAt(int i) {

    if (i < 0) {
      throw new IndexOutOfBoundsException("i < 0, " + i);
    }

    if (i >= length) {
      throw new IndexOutOfBoundsException("i >= length, " + i + " >= " + length);
    }

    return data[offset + i];
  }

  /**
   * Gets the length of bytes
   */
  public int length() {
    return length;
  }

  /**
   * Returns a portion of the Bytes object
   *
   * @param start index of subsequence start (inclusive)
   * @param end index of subsequence end (exclusive)
   */
  public Bytes subSequence(int start, int end) {
    if (start > end || start < 0 || end > length) {
      throw new IllegalArgumentException("Bad start and/end start = " + start + " end=" + end
          + " offset=" + offset + " length=" + length);
    }
    return new Bytes(data, offset + start, end - start);
  }

  /**
   * Returns a byte array containing a copy of the bytes
   */
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

  /**
   * Compares this to the given bytes, byte by byte, returning a negative, zero, or positive result
   * if the first sequence is less than, equal to, or greater than the second. The comparison is
   * performed starting with the first byte of each sequence, and proceeds until a pair of bytes
   * differs, or one sequence runs out of byte (is shorter). A shorter sequence is considered less
   * than a longer one.
   *
   * @param b1 first byte sequence to compare
   * @param b2 second byte sequence to compare
   * @return comparison result
   */
  @Override
  public final int compareTo(Bytes other) {
    int minLen = Math.min(this.length(), other.length());

    for (int i = 0; i < minLen; i++) {
      int a = (this.byteAt(i) & 0xff);
      int b = (other.byteAt(i) & 0xff);

      if (a != b) {
        return a - b;
      }
    }
    return this.length() - other.length();
  }

  /**
   * Returns true if this Bytes object equals another.
   */
  @Override
  public final boolean equals(Object other) {
    if (other instanceof Bytes) {
      Bytes ob = (Bytes) other;

      if (this == other) {
        return true;
      }

      if (length() != ob.length()) {
        return false;
      }

      return compareTo(ob) == 0;
    }
    return false;
  }

  @Override
  public final int hashCode() {
    if (hashCode == null) {
      int hash = 1;
      for (int i = 0; i < length(); i++) {
        hash = (31 * hash) + byteAt(i);
      }
      hashCode = hash;
    }
    return hashCode;
  }

  /**
   * Creates a Bytes object by copying the data of the given byte array
   */
  public static final Bytes of(byte[] array) {
    Objects.requireNonNull(array);
    if (array.length == 0) {
      return EMPTY;
    }
    byte[] copy = new byte[array.length];
    System.arraycopy(array, 0, copy, 0, array.length);
    return new Bytes(copy);
  }

  /**
   * Creates a Bytes object by copying the data of a subsequence of the given byte array
   *
   * @param data Byte data
   * @param offset Starting offset in byte array (inclusive)
   * @param length Number of bytes to include
   */
  public static final Bytes of(byte[] data, int offset, int length) {
    Objects.requireNonNull(data);
    if (length == 0) {
      return EMPTY;
    }
    byte[] copy = new byte[length];
    System.arraycopy(data, offset, copy, 0, length);
    return new Bytes(copy);
  }

  /**
   * Creates a Bytes object by copying the data of the given ByteBuffer
   */
  public static final Bytes of(ByteBuffer bb) {
    Objects.requireNonNull(bb);
    if (bb.remaining() == 0) {
      return EMPTY;
    }
    byte[] data = new byte[bb.remaining()];
    // duplicate so that it does not change position
    bb.duplicate().get(data);
    return new Bytes(data);
  }

  /**
   * Creates a Bytes object by copying the value of the given String
   */
  public static final Bytes of(String s) {
    Objects.requireNonNull(s);
    if (s.length() == 0) {
      return EMPTY;
    }
    byte[] data = s.getBytes(StandardCharsets.UTF_8);
    return new Bytes(data);
  }

  /**
   * Creates a Bytes object by copying the value of the given String with a given charset
   */
  public static final Bytes of(String s, Charset c) {
    Objects.requireNonNull(s);
    Objects.requireNonNull(c);
    if (s.length() == 0) {
      return EMPTY;
    }
    byte[] data = s.getBytes(c);
    return new Bytes(data);
  }

  /**
   * Writes Bytes to DataOutput
   *
   * @param out DataOutput
   * @param b Bytes
   */
  public static final void write(DataOutput out, Bytes b) throws IOException {
    writeUtil.writeVInt(out, b.length());
    for (int i = 0; i < b.length(); i++) {
      out.write(b.byteAt(i) & 0xff);
    }
  }

  /**
   * Wraps data input as Bytes
   *
   * @param in DataInput
   * @return Bytes
   */
  public static final Bytes read(DataInput in) throws IOException {
    int len = writeUtil.readVInt(in);
    byte[] b = new byte[len];
    in.readFully(b);
    return of(b);
  }

  /**
   * Provides an efficient and reusable way to build immutable Bytes objects.
   */
  public static BytesBuilder newBuilder() {
    return new BytesBuilder();
  }

  /**
   * @param initialCapacity The initial size of the byte builders internal array.
   */
  public static BytesBuilder newBuilder(int initialCapacity) {
    return new BytesBuilder(initialCapacity);
  }

  /**
   * Concatenates of list of Bytes objects to create a byte array
   *
   * @param listOfBytes Bytes objects to concatenate
   * @return Bytes
   */
  public static final Bytes concat(Bytes... listOfBytes) {
    try {
      // TODO calculate exact array size needed
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);

      for (Bytes b : listOfBytes) {
        writeUtil.writeVInt(dos, b.length());
        dos.write(b.toArray());
      }

      dos.close();
      return of(baos.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Splits a bytes object into several bytes objects
   *
   * @param b Original bytes object
   * @return List of bytes objects
   */
  public static final List<Bytes> split(Bytes b) {
    ByteArrayInputStream bais;
    bais = new ByteArrayInputStream(b.toArray());

    DataInputStream dis = new DataInputStream(bais);

    ArrayList<Bytes> ret = new ArrayList<>();

    try {
      while (true) {
        int len = writeUtil.readVInt(dis);
        // TODO could get pointers into original byte seq
        byte[] field = new byte[len];
        dis.readFully(field);
        ret.add(of(field));
      }
    } catch (EOFException ee) {
      // at end of file
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ret;
  }
}
