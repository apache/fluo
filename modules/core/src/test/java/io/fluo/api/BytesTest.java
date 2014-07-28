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
package io.fluo.api;

import io.fluo.impl.ArrayBytes;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for Bytes & ArrayBytes classes
 */
public class BytesTest {

  @Test
  public void testBytesWrap() {
    
    String s1 = "test1";
    Bytes b1 = Bytes.wrap(s1);
    Assert.assertArrayEquals(s1.getBytes(), b1.toArray());
    Assert.assertEquals(s1, b1.toString());
    
    String s2 = "test2";
    ByteBuffer bb = ByteBuffer.wrap(s2.getBytes());
    Bytes b2 = Bytes.wrap(bb);
    Assert.assertArrayEquals(s2.getBytes(), b2.toArray());
    Assert.assertEquals(s2, b2.toString());
    
    String s3 = "test3";
    ByteSequence bs = new ArrayByteSequence(s3);
    ArrayBytes b3 = new ArrayBytes(bs);
    Assert.assertArrayEquals(s3.getBytes(), b3.toArray());
    Assert.assertEquals(s3, b3.toString());
    Assert.assertEquals(bs, b3.toByteSequence());
    
    String s4 = "test4";
    Bytes b4 = Bytes.wrap(s4.getBytes());
    Assert.assertArrayEquals(s4.getBytes(), b4.toArray());
    Assert.assertEquals(s4, b4.toString());
    
    String s5 = "test5";
    Text t5 = new Text(s5);
    ArrayBytes b5 = new ArrayBytes(s5);
    Assert.assertEquals(t5, b5.toText());
  }
  
  @Test
  public void testConcatSplit() {
    
    Bytes b1 = Bytes.wrap("str1");
    Bytes b2 = Bytes.wrap("string2");
    Bytes b3 = Bytes.wrap("s3");
    Bytes ball = Bytes.concat(b1, b2, b3);
    
    List<Bytes> blist = Bytes.split(ball);
    
    Assert.assertEquals(b1, blist.get(0));
    Assert.assertEquals(b2, blist.get(1));
    Assert.assertEquals(b3, blist.get(2));
  }
}
