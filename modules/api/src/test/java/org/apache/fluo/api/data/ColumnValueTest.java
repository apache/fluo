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

package org.apache.fluo.api.data;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

public class ColumnValueTest {



  @Test
  public void testEquals() {
    Column c1 = new Column("f1", "q1");
    Column c2 = new Column("f2", "q1");
    Column c3 = new Column("f1", "q2");
    Column c4 = new Column("f2", "q2");

    ColumnValue cv = new ColumnValue(c1, "v1");

    Assert.assertEquals(cv, cv);
    Assert.assertEquals(cv.hashCode(), cv.hashCode());
    Assert.assertEquals(new ColumnValue(c1, "v1"), cv);
    Assert.assertEquals(new ColumnValue(c1, "v1").hashCode(), cv.hashCode());
    Assert.assertNotEquals(new ColumnValue(c1, "v2"), cv);
    Assert.assertNotEquals(new ColumnValue(c1, "v2").hashCode(), cv.hashCode());

    for (Column c : Arrays.asList(c2, c3, c4)) {
      for (String v : Arrays.asList("v1", "v2")) {
        ColumnValue ocv = new ColumnValue(c, v);
        Assert.assertNotEquals(ocv, cv);
        Assert.assertNotEquals(ocv.hashCode(), cv.hashCode());
      }
    }

    Assert.assertNotEquals(cv, c1);
    Assert.assertNotEquals(cv, "v1");
  }

  @Test
  public void testCompare() {
    Column c1 = new Column("f1", "q1");
    Column c2 = new Column("f2", "q1");

    ColumnValue cv1 = new ColumnValue(c1, "v1");
    ColumnValue cv2 = new ColumnValue(c2, "v1");
    ColumnValue cv3 = new ColumnValue(c1, "v2");
    ColumnValue cv4 = new ColumnValue(c1, "v1");

    Assert.assertEquals(0, cv1.compareTo(cv1));
    Assert.assertEquals(0, cv1.compareTo(cv4));
    Assert.assertTrue(cv1.compareTo(cv2) < 0);
    Assert.assertTrue(cv2.compareTo(cv1) > 0);
    Assert.assertTrue(cv1.compareTo(cv3) < 0);
    Assert.assertTrue(cv3.compareTo(cv1) > 0);
  }

  @Test
  public void testGet() {
    Column c1 = new Column("f1", "q1");
    ColumnValue cv1 = new ColumnValue(c1, "v1");

    Assert.assertEquals("v1", cv1.getsValue());
    Assert.assertEquals("v1", cv1.getValue().toString());
    Assert.assertSame(c1, cv1.getColumn());
  }
}
