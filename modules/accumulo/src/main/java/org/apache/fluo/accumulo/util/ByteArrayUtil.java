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

import org.apache.fluo.api.data.Bytes;

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
   * Concatenate several byte arrays into one
   * 
   * @param byteArrays List of byte arrays
   * @return concatenated byte array
   */
  public static byte[] concat(byte[]... byteArrays) {
    Bytes[] bs = new Bytes[byteArrays.length];
    for (int i = 0; i < byteArrays.length; i++) {
      bs[i] = Bytes.of(byteArrays[i]);
    }
    return Bytes.concat(bs).toArray();
  }
}
