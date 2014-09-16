/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.api.data;

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

import org.apache.hadoop.io.WritableUtils;

/**
 * Represents a sequence of bytes in Fluo
 */
public abstract class Bytes implements Comparable<Bytes> {
  
  public static final Bytes EMPTY = Bytes.wrap(new byte[0]);
  
  public Bytes() {}

  /**
   * Gets a byte within this sequence of bytes
   *
   * @param i index into sequence
   * @return byte
   * @throws IllegalArgumentException if i is out of range
   */
  public abstract byte byteAt(int i);

  /**
   * Gets the length of this sequence.
   *
   * @return sequence length
   */
  public abstract int length();

  /**
   * Returns a portion of this bytes sequence.
   *
   * @param start index of subsequence start (inclusive)
   * @param end index of subsequence end (exclusive)
   */
  public abstract Bytes subSequence(int start, int end);

  /**
   * Returns a byte array containing the bytes in this sequence. This method
   * may copy the sequence data or may return a backing byte array directly.
   *
   * @return byte array
   */
  public abstract byte[] toArray();
  
  /**
   * Determines whether this sequence is backed by a byte array.
   *
   * @return true if sequence is backed by a byte array
   */
  public abstract boolean isBackedByArray();

  /**
   * Gets the backing byte array for this sequence.
   *
   * @return byte array
   */
  public abstract byte[] getBackingArray();

  /**
   * Gets the offset for this byte sequence. This value represents the starting
   * point for the sequence in the backing array, if there is one.
   *
   * @return offset (inclusive)
   */
  public abstract int offset();

  /**
   * Compares the two given byte sequences, byte by byte, returning a negative,
   * zero, or positive result if the first sequence is less than, equal to, or
   * greater than the second. The comparison is performed starting with the
   * first byte of each sequence, and proceeds until a pair of bytes differs,
   * or one sequence runs out of byte (is shorter). A shorter sequence is
   * considered less than a longer one.
   *
   * @param b1 first byte sequence to compare
   * @param b2 second byte sequence to compare
   * @return comparison result
   */
  public static int compareBytes(Bytes b1, Bytes b2) {

    int minLen = Math.min(b1.length(), b2.length());

    for (int i = 0; i < minLen; i++) {
      int a = (b1.byteAt(i) & 0xff);
      int b = (b2.byteAt(i) & 0xff);

      if (a != b) {
        return a - b;
      }
    }
    return b1.length() - b2.length();
  }

  /**
   * Compares this Bytes to another.
   *
   * @param other Bytes
   * @return comparison result
   */
  @Override
  public int compareTo(Bytes other) {      
    return compareBytes(this, other);
  }

  /**
   * Determines if this Bytes equals another.
   *
   * @param other object
   * @return true if equal
   */
  @Override
  public boolean equals(Object other) {
    if (other instanceof Bytes) {
      Bytes ob = (Bytes) other;

      if (this == other)
        return true;

      if (length() != ob.length())
        return false;

      return compareTo(ob) == 0;
    }
    return false;
  }

  /**
   * Computes hash code of Bytes
   * 
   * @return hash code
   */
  @Override
  public int hashCode() {
    int hash = 1;
    if (isBackedByArray()) {
      byte[] data = getBackingArray();
      int end = offset() + length();
      for (int i = offset(); i < end; i++)
        hash = (31 * hash) + data[i];
    } else {
      for (int i = 0; i < length(); i++)
        hash = (31 * hash) + byteAt(i);
    }
    return hash;
  }
  
  /**
   * Wraps byte array as Bytes
   * 
   * @param array byte array
   * @return Bytes
   */
  public static Bytes wrap(byte[] array) {
    return new ArrayBytes(array);
  }
  
  /**
   * Creates a Bytes object by wrapping a subsequence of the given byte array. 
   * The given byte array is used directly as the backing array, so later changes
   * made to the (relevant portion of the) array reflect into the new Byte array.
   *
   * @param data Byte data
   * @param offset Starting offset in byte array (inclusive)
   * @param length Number of bytes to include
   */
  public static Bytes wrap(byte data[], int offset, int length) {
    return new ArrayBytes(data, offset, length);
  }

  /**
   * Wraps ByteBuffer as Bytes
   * 
   * @param bb ByteBuffer
   * @return Bytes
   */
  public static Bytes wrap(ByteBuffer bb) {
    return new ArrayBytes(bb);
  }
  
  /**
   * Wraps a UTF-8 String as Bytes
   * 
   * @param s String
   * @return Bytes
   */
  public static Bytes wrap(String s) {
    return new ArrayBytes(s);
  }
  
  /**
   * Wraps a String with a given charset as Bytes
   * 
   * @param s String
   * @return Bytes
   */
  public static Bytes wrap(String s, Charset c) {
    return new ArrayBytes(s, c);
  }
  
  /**
   * Writes Bytes to DataOutput 
   * 
   * @param out DataOutput
   * @param b Bytes
   * @throws IOException
   */
  public static void write(DataOutput out, Bytes b) throws IOException {
    WritableUtils.writeVInt(out, b.length());
    if (b.isBackedByArray()) {
      out.write(b.getBackingArray(), b.offset(), b.length());
    } else {
      for (int i = 0; i < b.length(); i++) {
        out.write(b.byteAt(i) & 0xff);
      }
    }
  }

  /**
   * Wraps data input as Bytes
   * 
   * @param in DataInput
   * @return Bytes
   * @throws IOException
   */
  public static Bytes read(DataInput in) throws IOException {
    int len = WritableUtils.readVInt(in);
    byte b[] = new byte[len];
    in.readFully(b);
    return wrap(b);
  }
  
  /**
   * Concatenates of list of Bytes objects to create a byte array
   * 
   * @param listOfBytes
   * @return Bytes
   */
  public static Bytes concat(Bytes... listOfBytes) {
    try {
      // TODO calculate exact array size needed
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      
      for (Bytes b : listOfBytes) {
        WritableUtils.writeVInt(dos, b.length());
        if (b.isBackedByArray()) {
          dos.write(b.getBackingArray(), b.offset(), b.length());
        } else {
          dos.write(b.toArray());
        }
      }
      
      dos.close();
      return wrap(baos.toByteArray());
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
  public static List<Bytes> split(Bytes b) {
    ByteArrayInputStream bais;
    if (b.isBackedByArray())
      bais = new ByteArrayInputStream(b.getBackingArray(), b.offset(), b.length());
    else
      bais = new ByteArrayInputStream(b.toArray());
    
    DataInputStream dis = new DataInputStream(bais);
    
    ArrayList<Bytes> ret = new ArrayList<>();
    
    try {
      while (true) {
        int len = WritableUtils.readVInt(dis);
        // TODO could get pointers into original byte seq
        byte field[] = new byte[len];
        dis.readFully(field);
        ret.add(wrap(field));
      }
    } catch (EOFException ee) {
      
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ret;
  }
  
  /**
  * An implementation of {@link Bytes} that uses a backing byte array.
  */
  private static class ArrayBytes extends Bytes implements Serializable {

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
    private ArrayBytes(byte data[]) {
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
    private ArrayBytes(byte data[], int offset, int length) {
      if (offset < 0 || offset > data.length || length < 0 || (offset + length) > data.length) {
        throw new IllegalArgumentException(" Bad offset and/or length data.length = " + data.length + " offset = " + offset + " length = " + length);
      }
      this.data = data;
      this.offset = offset;
      this.length = length;
    }

    /**
     * Creates a new ArrayBytes from the given string. The bytes are determined from
     * the string using UTF-8 encoding
     *
     * @param s String to represent as Bytes
     */
    private ArrayBytes(String s) {
      this(s.getBytes(StandardCharsets.UTF_8));
    }
    
    /**
     * Creates a new ArrayBytes from the given string. The bytes are determined from
     * the string using the specified charset
     *
     * @param s String to represent as Bytes
     * @param cs Charset
     */
    private ArrayBytes(String s, Charset cs) {
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
    private ArrayBytes(ByteBuffer buffer) {
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
    @Override
    public String toString() {
      return new String(data, offset, length, StandardCharsets.UTF_8);
    }
  }
}


