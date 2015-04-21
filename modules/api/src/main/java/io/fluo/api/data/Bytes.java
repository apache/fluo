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
package io.fluo.api.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Preconditions;

/**
 * Represents bytes in Fluo. Similar to an Accumulo ByteSequence. Bytes is immutable after it is
 * created. {@link Bytes.EMPTY} is used to represent a Bytes object with no data.
 */
public abstract class Bytes implements Comparable<Bytes> {

  private static final String BYTES_FACTORY_CLASS = "io.fluo.accumulo.data.MutableBytesFactory";
  private static final String WRITE_UTIL_CLASS = "io.fluo.accumulo.data.WriteUtilImpl";

  public interface BytesFactory {
    Bytes get(byte[] data);
  }

  public interface WriteUtil {
    void writeVInt(DataOutput stream, int i) throws IOException;

    int readVInt(DataInput stream) throws IOException;
  }

  private static BytesFactory bytesFactory;
  private static WriteUtil writeUtil;

  static {
    try {
      bytesFactory =
          (BytesFactory) Class.forName(BYTES_FACTORY_CLASS).getDeclaredConstructor().newInstance();
      writeUtil =
          (WriteUtil) Class.forName(WRITE_UTIL_CLASS).getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public static final Bytes EMPTY = Bytes.of(new byte[0]);

  private Integer hashCode = null;

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
   * Gets the length of bytes
   */
  public abstract int length();

  /**
   * Returns a portion of the Bytes object
   * 
   * @param start index of subsequence start (inclusive)
   * @param end index of subsequence end (exclusive)
   */
  public abstract Bytes subSequence(int start, int end);

  /**
   * Returns a byte array containing a copy of the bytes
   */
  public abstract byte[] toArray();

  /**
   * Compares the two given byte sequences, byte by byte, returning a negative, zero, or positive
   * result if the first sequence is less than, equal to, or greater than the second. The comparison
   * is performed starting with the first byte of each sequence, and proceeds until a pair of bytes
   * differs, or one sequence runs out of byte (is shorter). A shorter sequence is considered less
   * than a longer one.
   * 
   * @param b1 first byte sequence to compare
   * @param b2 second byte sequence to compare
   * @return comparison result
   */
  final public static int compareBytes(Bytes b1, Bytes b2) {

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
   * Compares this Bytes object to another.
   */
  @Override
  final public int compareTo(Bytes other) {
    return compareBytes(this, other);
  }

  /**
   * Returns true if this Bytes object equals another.
   */
  @Override
  final public boolean equals(Object other) {
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

  @Override
  final public int hashCode() {
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
  final public static Bytes of(byte[] array) {
    Preconditions.checkNotNull(array);
    byte[] copy = new byte[array.length];
    System.arraycopy(array, 0, copy, 0, array.length);
    return bytesFactory.get(copy);
  }

  /**
   * Creates a Bytes object by copying the data of a subsequence of the given byte array
   * 
   * @param data Byte data
   * @param offset Starting offset in byte array (inclusive)
   * @param length Number of bytes to include
   */
  final public static Bytes of(byte data[], int offset, int length) {
    Preconditions.checkNotNull(data);
    byte[] copy = new byte[length];
    System.arraycopy(data, offset, copy, 0, length);
    return bytesFactory.get(copy);
  }

  /**
   * Creates a Bytes object by copying the data of the given ByteBuffer
   */
  final public static Bytes of(ByteBuffer bb) {
    Preconditions.checkNotNull(bb);
    byte[] data = new byte[bb.remaining()];
    bb.get(data);
    return bytesFactory.get(data);
  }

  /**
   * Creates a Bytes object by copying the value of the given String
   */
  final public static Bytes of(String s) {
    Preconditions.checkNotNull(s);
    byte[] data = s.getBytes(StandardCharsets.UTF_8);
    return bytesFactory.get(data);
  }

  /**
   * Creates a Bytes object by copying the value of the given String with a given charset
   */
  final public static Bytes of(String s, Charset c) {
    Preconditions.checkNotNull(s);
    Preconditions.checkNotNull(c);
    byte[] data = s.getBytes(c);
    return bytesFactory.get(data);
  }

  /**
   * Writes Bytes to DataOutput
   * 
   * @param out DataOutput
   * @param b Bytes
   * @throws IOException
   */
  final public static void write(DataOutput out, Bytes b) throws IOException {
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
   * @throws IOException
   */
  final public static Bytes read(DataInput in) throws IOException {
    int len = writeUtil.readVInt(in);
    byte b[] = new byte[len];
    in.readFully(b);
    return of(b);
  }

  /**
   * Concatenates of list of Bytes objects to create a byte array
   * 
   * @param listOfBytes
   * @return Bytes
   */
  final public static Bytes concat(Bytes... listOfBytes) {
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
  final public static List<Bytes> split(Bytes b) {
    ByteArrayInputStream bais;
    bais = new ByteArrayInputStream(b.toArray());

    DataInputStream dis = new DataInputStream(bais);

    ArrayList<Bytes> ret = new ArrayList<>();

    try {
      while (true) {
        int len = writeUtil.readVInt(dis);
        // TODO could get pointers into original byte seq
        byte field[] = new byte[len];
        dis.readFully(field);
        ret.add(of(field));
      }
    } catch (EOFException ee) {

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ret;
  }
}
