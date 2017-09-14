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

import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.fluo.accumulo.util.ByteArrayUtil;
import org.apache.fluo.api.data.Bytes;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for ByteUtil class
 */
public class ByteUtilTest {

  @Test
  public void testHadoopText() {
    String s1 = "test1";
    Text t1 = new Text(s1);
    Bytes b1 = ByteUtil.toBytes(t1);
    Assert.assertEquals(Bytes.of(s1), b1);
    Assert.assertEquals(t1, ByteUtil.toText(b1));
  }

  @Test
  public void testByteSequence() {
    String s2 = "test2";
    ByteSequence bs2 = new ArrayByteSequence(s2);
    Bytes b2 = ByteUtil.toBytes(bs2);
    Assert.assertEquals(Bytes.of(s2), b2);
    Assert.assertEquals(bs2, ByteUtil.toByteSequence(b2));
  }

  @Test
  public void testConcatSplit() {

    Bytes b1 = Bytes.of("str1");
    Bytes b2 = Bytes.of("string2");
    Bytes b3 = Bytes.of("s3");
    Bytes b4 = Bytes.of("testinggreaterthan128characterstestinggreaterthan128characters"
        + "testinggreaterthan128characterstestinggreaterthan128characters"
        + "testinggreaterthan128characters"); // 155 length

    byte[] ball = ByteArrayUtil.concat(b1, b2, b3, b4);

    List<Bytes> blist = ByteArrayUtil.split(ball);

    Assert.assertEquals(4, blist.size());
    Assert.assertEquals(b1, blist.get(0));
    Assert.assertEquals(b2, blist.get(1));
    Assert.assertEquals(b3, blist.get(2));
    Assert.assertEquals(b4, blist.get(3));

    // test two args
    blist = ByteArrayUtil.split(ByteArrayUtil.concat(b1, b2));
    Assert.assertEquals(2, blist.size());
    Assert.assertEquals(b1, blist.get(0));
    Assert.assertEquals(b2, blist.get(1));

    blist = ByteArrayUtil.split(ByteArrayUtil.concat(b4, b2));
    Assert.assertEquals(2, blist.size());
    Assert.assertEquals(b4, blist.get(0));
    Assert.assertEquals(b2, blist.get(1));

    blist = ByteArrayUtil.split(ByteArrayUtil.concat(b1, b4));
    Assert.assertEquals(2, blist.size());
    Assert.assertEquals(b1, blist.get(0));
    Assert.assertEquals(b4, blist.get(1));

  }
}
