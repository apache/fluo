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
package org.apache.accumulo.accismus.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

/**
 * 
 */
public class ByteUtil {

  public static byte[] concat(ByteSequence... byteArrays) {
    
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(baos);
      
      for (ByteSequence bs : byteArrays) {
        dos.writeInt(bs.length());
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
        int len = dis.readInt();
        
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

}
