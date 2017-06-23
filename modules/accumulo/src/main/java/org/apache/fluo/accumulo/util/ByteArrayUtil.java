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

package org.apache.fluo.accumulo.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.hadoop.io.WritableUtils;

/**
 * Utilities for manipulating byte arrays
 */
public class ByteArrayUtil {

  private ByteArrayUtil() {}

  /**
   * Encode long as byte array
   * 
   * @param v Long value
   * @return byte array
   */
  public static byte[] encode(long v) {
    byte[] ba = new byte[8];
    encode(ba, 0, v);
    return ba;
  }

  /**
   * Encode a long into a byte array at an offset
   * 
   * @param ba Byte array
   * @param offset Offset
   * @param v Long value
   * @return byte array given in input
   */
  public static byte[] encode(byte[] ba, int offset, long v) {
    ba[offset + 0] = (byte) (v >>> 56);
    ba[offset + 1] = (byte) (v >>> 48);
    ba[offset + 2] = (byte) (v >>> 40);
    ba[offset + 3] = (byte) (v >>> 32);
    ba[offset + 4] = (byte) (v >>> 24);
    ba[offset + 5] = (byte) (v >>> 16);
    ba[offset + 6] = (byte) (v >>> 8);
    ba[offset + 7] = (byte) (v >>> 0);
    return ba;
  }

  /**
   * Decode long from byte array at offset
   * 
   * @param ba byte array
   * @param offset Offset
   * @return long value
   */
  public static long decodeLong(byte[] ba, int offset) {
    return ((((long) ba[offset + 0] << 56) + ((long) (ba[offset + 1] & 255) << 48)
        + ((long) (ba[offset + 2] & 255) << 40) + ((long) (ba[offset + 3] & 255) << 32)
        + ((long) (ba[offset + 4] & 255) << 24) + ((ba[offset + 5] & 255) << 16)
        + ((ba[offset + 6] & 255) << 8) + ((ba[offset + 7] & 255) << 0)));
  }

  /**
   * Decode long from byte array
   * 
   * @param ba byte array
   * @return long value
   */
  public static long decodeLong(byte[] ba) {
    return decodeLong(ba, 0);
  }

  /**
   * Concatenates of list of Bytes objects to create a byte array
   *
   * @param listOfBytes Bytes objects to concatenate
   * @return Bytes
   */
  public static byte[] concat(Bytes... listOfBytes) {
    int offset = 0;
    int size = 0;

    for (Bytes b : listOfBytes) {
      size += b.length() + checkVlen(b.length());
    }

    byte[] data = new byte[size];
    for (Bytes b : listOfBytes) {
      offset = writeVint(data, offset, b.length());
      b.copyTo(0, b.length(), data, offset);
      offset += b.length();
    }
    return data;
  }

  /**
   * Writes a vInt directly to a byte array
   * 
   * @param dest The destination array for the vInt to be written to
   * @param offset The location where to write the vInt to
   * @param i The Value being written into byte array
   * @return Returns the new offset location
   */
  public static int writeVint(byte[] dest, int offset, int i) {
    if (i >= -112 && i <= 127) {
      dest[offset++] = (byte) i;
    } else {
      int len = -112;
      if (i < 0) {
        i ^= -1L; // take one's complement'
        len = -120;
      }

      long tmp = i;
      while (tmp != 0) {
        tmp = tmp >> 8;
        len--;
      }

      dest[offset++] = (byte) len;

      len = (len < -120) ? -(len + 120) : -(len + 112);

      for (int idx = len; idx != 0; idx--) {
        int shiftbits = (idx - 1) * 8;
        long mask = 0xFFL << shiftbits;
        dest[offset++] = (byte) ((i & mask) >> shiftbits);
      }
    }

    return offset;
  }

  /**
   * Determines the number bytes required to store a variable length
   * 
   * @param i length of Bytes
   * @return number of bytes needed
   */
  public static int checkVlen(int i) {
    int count = 0;
    if (i >= -112 && i <= 127) {
      return 1;
    } else {
      int len = -112;
      if (i < 0) {
        i ^= -1L; // take one's complement'
        len = -120;
      }

      long tmp = i;
      while (tmp != 0) {
        tmp = tmp >> 8;
        len--;
      }

      count++;

      len = (len < -120) ? -(len + 120) : -(len + 112);

      while (len != 0) {
        count++;
        len--;
      }

      return count;
    }
  }

  public static final List<Bytes> split(byte[] b) {

    ArrayList<Bytes> ret = new ArrayList<>();

    try (InputStream in = new ByteArrayInputStream(b)) {
      DataInputStream dis = new DataInputStream(in);

      BytesBuilder builder = Bytes.builder(b.length);

      while (true) {
        int len = WritableUtils.readVInt(dis);
        builder.append(dis, len);
        ret.add(builder.toBytes());
        builder.setLength(0);
      }
    } catch (EOFException ee) {
      // at end of file
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return ret;
  }

}
