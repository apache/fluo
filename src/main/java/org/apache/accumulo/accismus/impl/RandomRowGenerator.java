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

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;


class RandomRowGenerator {
  
  Random rand = new Random();
  
  public static interface DataSource {
    Iterator<Entry<Key,Value>> scan(Text start, Text end);
  }

  RandomRowGenerator(Random rand2) {
    this.rand = rand2;
  }

  private static void appendZeros(ByteArrayOutputStream baos, int num) {
    for (int i = 0; i < num; i++)
      baos.write(0);
  }

  @SuppressWarnings("unused")
  private static void print(String msg, Text row) {
    System.out.print(msg);

    for (int i = 0; i < row.getLength(); i++) {
      byte pb = row.getBytes()[i];
      if (pb < 32 || pb > 126)
        pb = '.';
      
      System.out.printf("%02x:%c ", row.getBytes()[i], pb);
      

    }
    
    System.out.println();
  }
  
  private Text pickRandom(Text minBS, Text maxBS) {
    // print("min: ", minBS);
    // print("max: ", maxBS);
    
    if (minBS.equals(maxBS))
      throw new IllegalArgumentException();

    ByteArrayOutputStream startOS = new ByteArrayOutputStream();
    startOS.write(0); // add a leading zero so bigint does not think its negative
    startOS.write(minBS.getBytes(), 0, minBS.getLength());
    
    ByteArrayOutputStream endOS = new ByteArrayOutputStream();
    endOS.write(0);// add a leading zero so bigint does not think its negative
    endOS.write(maxBS.getBytes(), 0, maxBS.getLength());
    
    // make the numbers of the same magnitude
    if (startOS.size() < endOS.size())
      appendZeros(startOS, endOS.size() - startOS.size());
    else if (endOS.size() < startOS.size())
      appendZeros(endOS, startOS.size() - endOS.size());
    
    BigInteger min = new BigInteger(startOS.toByteArray());
    BigInteger max = new BigInteger(endOS.toByteArray());
    
    BigInteger diff = max.subtract(min);
    
    BigInteger rnum;
    do {
      rnum = new BigInteger(diff.bitLength(), rand);
    } while (rnum.compareTo(diff) >= 0);
    
    byte[] ba = rnum.add(min).toByteArray();
    
    Text ret = new Text();
    
    if (ba.length == startOS.size()) {
      if (ba[0] != 0)
        throw new RuntimeException();
      
      // big int added a zero so it would not be negative, drop it
      ret.set(ba, 1, ba.length - 1);
    } else {
      int expLen = Math.max(minBS.getLength(), maxBS.getLength());
      // big int will drop leading 0x0 bytes
      for (int i = ba.length; i < expLen; i++) {
        ret.append(new byte[] {0}, 0, 1);
      }
      
      ret.append(ba, 0, ba.length);
    }
    
    // remove trailing 0x0 bytes
    while (ret.getLength() > 0 && ret.getBytes()[ret.getLength() - 1] == 0 && ret.compareTo(minBS) > 0) {
      Text t = new Text();
      t.set(ret.getBytes(), 0, ret.getLength() - 1);
      ret = t;
    }
    
    return ret;
  }
  
  Text pickRandomRow(DataSource scanner, Text start, Text end) {
    if (start == null) {
      start = new Text(new byte[] {0});
    } else {
      start.append(new byte[] {0}, 0, 1);
    }
    
    if (end == null) {
      end = new Text(new byte[] {(byte) 0xff});
    }


    Iterator<Entry<Key,Value>> iter = scanner.scan(start, end);
    
    if (iter.hasNext()) {
      Text first = iter.next().getKey().getRow();
      
      if (!iter.hasNext()) {
        return first;
      }
      
      Text randRow = pickRandom(first, end);
      
      while (!scanner.scan(randRow, end).hasNext()) {
        randRow = pickRandom(first, randRow);
        if (randRow.equals(first)) {
          return first;
        }
      }
      
      return randRow;
    }
    
    return null;
  }
}
