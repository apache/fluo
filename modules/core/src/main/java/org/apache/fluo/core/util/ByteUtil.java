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

package org.apache.fluo.core.util;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

/**
 * Utilities for modifying byte arrays and converting Bytes objects to external formats
 */
public class ByteUtil {

  public static final byte[] EMPTY = new byte[0];

  private ByteUtil() {}

  /**
   * Convert from Bytes to Hadoop Text object
   */
  public static Text toText(Bytes b) {
    return new Text(b.toArray());
  }

  /**
   * Convert from Hadoop Text to Bytes
   */
  public static Bytes toBytes(Text t) {
    return Bytes.of(t.getBytes(), 0, t.getLength());
  }

  /**
   * Converts from ByteSequence to Bytes. If the ByteSequence has a backing array, that array (and
   * the buffer's offset and limit) are used. Otherwise, a new backing array is created.
   *
   * @param bs ByteSequence
   * @return Bytes object
   */
  public static Bytes toBytes(ByteSequence bs) {
    if (bs.length() == 0) {
      return Bytes.EMPTY;
    }

    if (bs.isBackedByArray()) {
      return Bytes.of(bs.getBackingArray(), bs.offset(), bs.length());
    } else {
      return Bytes.of(bs.toArray(), 0, bs.length());
    }
  }

  /**
   * Convert from Bytes to ByteSequence
   */
  public static ByteSequence toByteSequence(Bytes b) {
    return new ArrayByteSequence(b.toArray());
  }

  public static byte[] toByteArray(Text text) {
    byte[] bytes = text.getBytes();
    if (bytes.length != text.getLength()) {
      bytes = new byte[text.getLength()];
      System.arraycopy(text.getBytes(), 0, bytes, 0, bytes.length);
    }
    return bytes;
  }

  public static void write(DataOutputStream out, Bytes b) throws IOException {
    WritableUtils.writeVInt(out, b.length());
    b.writeTo(out);
  }

  public static Bytes read(BytesBuilder bb, DataInputStream in) throws IOException {
    bb.setLength(0);
    int len = WritableUtils.readVInt(in);
    bb.append(in, len);
    return bb.toBytes();
  }

}
