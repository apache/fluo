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
package org.fluo.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

/**
 * 
 */
public class ByteUtil {

  public static byte[] concat(ByteSequence... byteArrays) {
    
    try {
      // TODO calculate exact array size needed
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      
      for (ByteSequence bs : byteArrays) {
        WritableUtils.writeVInt(dos, bs.length());
        if (bs.isBackedByArray()) {
          dos.write(bs.getBackingArray(), bs.offset(), bs.length());
        } else {
          // TODO avoid array conversion
          dos.write(bs.toArray());
        }
      }
      
      dos.close();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static List<ByteSequence> split(ByteSequence bs) {
    ByteArrayInputStream bais;
    if (bs.isBackedByArray())
      bais = new ByteArrayInputStream(bs.getBackingArray(), bs.offset(), bs.length());
    else
      bais = new ByteArrayInputStream(bs.toArray());
    
    DataInputStream dis = new DataInputStream(bais);
    
    ArrayList<ByteSequence> ret = new ArrayList<ByteSequence>();
    
    try {
      while (true) {
        int len = WritableUtils.readVInt(dis);
        
        // TODO could get pointers into original byte seq
        byte field[] = new byte[len];
        
        dis.readFully(field);
        ret.add(new ArrayByteSequence(field));
      }
    } catch (EOFException ee) {
      
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    
    return ret;
  }

  public static byte[] encode(long v) {
    byte ba[] = new byte[8];
    encode(ba, 0, v);
    return ba;
  }

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

  public static long decodeLong(byte[] ba, int offset) {
    return ((((long) ba[offset + 0] << 56) + ((long) (ba[offset + 1] & 255) << 48) + ((long) (ba[offset + 2] & 255) << 40)
        + ((long) (ba[offset + 3] & 255) << 32) + ((long) (ba[offset + 4] & 255) << 24) + ((ba[offset + 5] & 255) << 16) + ((ba[offset + 6] & 255) << 8) + ((ba[offset + 7] & 255) << 0)));
  }

  public static long decodeLong(byte[] ba) {
    return ((((long) ba[0] << 56) + ((long) (ba[1] & 255) << 48) + ((long) (ba[2] & 255) << 40) + ((long) (ba[3] & 255) << 32) + ((long) (ba[4] & 255) << 24)
        + ((ba[5] & 255) << 16) + ((ba[6] & 255) << 8) + ((ba[7] & 255) << 0)));
  
  }

  public static byte[] concat(byte[]... byteArrays) {
    ByteSequence[] bs = new ByteSequence[byteArrays.length];
    for (int i = 0; i < byteArrays.length; i++) {
      bs[i] = new ArrayByteSequence(byteArrays[i]);
    }
    
    return concat(bs);
  }

  public static Text toText(ByteSequence bs) {
    if (bs.isBackedByArray()) {
      Text t = new Text(TransactionImpl.EMPTY);
      t.set(bs.getBackingArray(), bs.offset(), bs.length());
      return t;
    } else {
      return new Text(bs.toArray());
    }
  }

  public static void write(DataOutput out, ByteSequence sequence) throws IOException {
    WritableUtils.writeVInt(out, sequence.length());
    if (sequence.isBackedByArray()) {
      out.write(sequence.getBackingArray(), sequence.offset(), sequence.length());
    } else {
      for (int i = 0; i < sequence.length(); i++) {
        out.write(sequence.byteAt(i) & 0xff);
      }
    }
    
  }
  
  public static ByteSequence read(DataInput in) throws IOException {
    int len = WritableUtils.readVInt(in);
    byte b[] = new byte[len];
    
    in.readFully(b);
    
    return new ArrayByteSequence(b);
  }

}
