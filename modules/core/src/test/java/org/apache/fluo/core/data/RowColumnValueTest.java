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

package org.apache.fluo.core.data;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.junit.Assert;
import org.junit.Test;

public class RowColumnValueTest {

  RowColumnValue rcv1 = new RowColumnValue("row1", new Column("fam1", "qual1"), "a");
  RowColumnValue rcv2 = new RowColumnValue("row1", new Column("fam1", "qual1"), "a");
  RowColumnValue rcv3 = new RowColumnValue("row2", new Column("fam1", "qual1"), "a");
  RowColumnValue rcv4 = new RowColumnValue("row1", new Column("fam1", "qual1"), "b");
  RowColumnValue rcv5 = new RowColumnValue("row1", new Column("fam2", "qual1"), "a");
  RowColumnValue rcv6 = new RowColumnValue("row2", new Column("fam2", "qual1"), "a");
  RowColumnValue rcv7 = new RowColumnValue("row2", new Column("fam2", "qual1"), "b");

  @Test
  public void testEquals() {
    for (RowColumnValue rcv : Arrays.asList(rcv1, rcv2)) {
      Assert.assertEquals(rcv1, rcv);
      Assert.assertEquals(rcv1.hashCode(), rcv.hashCode());
    }

    for (RowColumnValue rcv : Arrays.asList(rcv3, rcv4, rcv5, rcv6, rcv7)) {
      Assert.assertNotEquals(rcv1, rcv);
      Assert.assertNotEquals(rcv1.hashCode(), rcv.hashCode());
    }
  }

  @Test
  public void testGet() {
    RowColumnValue rcv1 = new RowColumnValue("row1", new Column("fam1", "qual1"), "a");

    Assert.assertEquals(rcv1.getRow(), Bytes.of("row1"));
    Assert.assertEquals(rcv1.getValue(), Bytes.of("a"));
    Assert.assertEquals(rcv1.getsValue(), "a");
    Assert.assertEquals(rcv1.getColumn(), new Column("fam1", "qual1"));
  }

  @Test
  public void testCompare() {
    Assert.assertTrue(rcv1.compareTo(rcv1) == 0);
    Assert.assertTrue(rcv1.compareTo(rcv2) == 0);
    Assert.assertTrue(rcv1.compareTo(rcv3) < 0);
    Assert.assertTrue(rcv3.compareTo(rcv1) > 0);
    Assert.assertTrue(rcv1.compareTo(rcv3) < 0);
    Assert.assertTrue(rcv3.compareTo(rcv1) > 0);
    Assert.assertTrue(rcv1.compareTo(rcv5) < 0);
    Assert.assertTrue(rcv5.compareTo(rcv1) > 0);
    Assert.assertTrue(rcv1.compareTo(rcv4) < 0);
    Assert.assertTrue(rcv4.compareTo(rcv1) > 0);

    List<RowColumnValue> l1 = Arrays.asList(rcv7, rcv6, rcv5, rcv4, rcv3, rcv2, rcv1);
    Collections.shuffle(l1);
    Collections.sort(l1);

    List<RowColumnValue> l2 = Arrays.asList(rcv1, rcv2, rcv4, rcv5, rcv3, rcv6, rcv7);

    Assert.assertEquals(l2, l1);
  }

  @Test
  public void testToString() {
    Assert.assertEquals("row1 fam1 qual1  a", rcv1.toString());
  }
}
