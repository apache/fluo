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

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link Column}
 */
public class ColumnTest {

  @Test
  public void testCreation() {
    Column col = new Column();
    Assert.assertFalse(col.isFamilySet());
    Assert.assertFalse(col.isQualifierSet());
    Assert.assertFalse(col.isVisibilitySet());
    Assert.assertSame(Bytes.EMPTY, col.getFamily());
    Assert.assertSame(Bytes.EMPTY, col.getQualifier());
    Assert.assertSame(Bytes.EMPTY, col.getVisibility());

    col = Column.EMPTY;
    Assert.assertFalse(col.isFamilySet());
    Assert.assertFalse(col.isQualifierSet());
    Assert.assertFalse(col.isVisibilitySet());

    Assert.assertEquals(new Column(), new Column());
    Assert.assertEquals(Column.EMPTY, Column.EMPTY);
    Assert.assertEquals(Column.EMPTY, new Column());

    Assert.assertEquals(new Column("a"), new Column(Bytes.of("a")));
    Assert.assertEquals(new Column("a"), new Column(Bytes.of("a"), Bytes.EMPTY, Bytes.EMPTY));
    Assert.assertEquals(new Column("a").hashCode(),
        new Column(Bytes.of("a"), Bytes.EMPTY, Bytes.EMPTY).hashCode());

    col = new Column("cf1");
    Assert.assertTrue(col.isFamilySet());
    Assert.assertFalse(col.isQualifierSet());
    Assert.assertFalse(col.isVisibilitySet());
    Assert.assertEquals(Bytes.of("cf1"), col.getFamily());
    Assert.assertSame(Bytes.EMPTY, col.getQualifier());
    Assert.assertSame(Bytes.EMPTY, col.getVisibility());
    Assert.assertEquals(new Column("cf1"), col);

    col = new Column("cf2", "cq2");
    Assert.assertTrue(col.isFamilySet());
    Assert.assertTrue(col.isQualifierSet());
    Assert.assertFalse(col.isVisibilitySet());
    Assert.assertEquals(Bytes.of("cf2"), col.getFamily());
    Assert.assertEquals(Bytes.of("cq2"), col.getQualifier());
    Assert.assertSame(Bytes.EMPTY, col.getVisibility());
    Assert.assertEquals(new Column("cf2", "cq2"), col);

    col = new Column("cf3", "cq3", "cv3");
    Assert.assertTrue(col.isFamilySet());
    Assert.assertTrue(col.isQualifierSet());
    Assert.assertTrue(col.isVisibilitySet());
    Assert.assertEquals(Bytes.of("cf3"), col.getFamily());
    Assert.assertEquals(Bytes.of("cq3"), col.getQualifier());
    Assert.assertEquals(Bytes.of("cv3"), col.getVisibility());
    Assert.assertEquals(new Column("cf3", "cq3", "cv3"), col);
  }

  @Test
  public void testCompare() {
    Column c1 = new Column("a", "b");
    Column c2 = new Column("a", "c");
    Column c3 = new Column("a", "b", "d");
    Column c4 = new Column("a");
    Column c5 = Column.EMPTY;
    Column c6 = new Column("a", "b");

    Assert.assertEquals(-1, c1.compareTo(c2));
    Assert.assertEquals(1, c2.compareTo(c1));
    Assert.assertEquals(0, c1.compareTo(c6));
    Assert.assertEquals(1, c1.compareTo(c5));
    Assert.assertEquals(-1, c4.compareTo(c1));
    Assert.assertEquals(-1, c1.compareTo(c3));
    Assert.assertEquals(1, c4.compareTo(c5));
    Assert.assertEquals(-1, c3.compareTo(c2));
  }

  @Test
  public void testStringGetters() {
    Column c0 = Column.EMPTY;
    Column c1 = new Column("cf");
    Column c2 = new Column("cf", "cq");
    Column c3 = new Column("cf", "cq", "cv");

    Assert.assertEquals("", c0.getsFamily());
    Assert.assertEquals("", c0.getsQualifier());
    Assert.assertEquals("", c0.getsVisibility());

    Assert.assertEquals("cf", c1.getsFamily());
    Assert.assertEquals("", c1.getsQualifier());
    Assert.assertEquals("", c1.getsVisibility());

    Assert.assertEquals("cf", c2.getsFamily());
    Assert.assertEquals("cq", c2.getsQualifier());
    Assert.assertEquals("", c2.getsVisibility());

    Assert.assertEquals("cf", c3.getsFamily());
    Assert.assertEquals("cq", c3.getsQualifier());
    Assert.assertEquals("cv", c3.getsVisibility());
  }
}
