/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.api.data;

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
    
    Assert.assertEquals(new Column("a"), new Column(Bytes.wrap("a")));
    Assert.assertEquals(new Column("a"), new Column(Bytes.wrap("a"), Bytes.EMPTY, Bytes.EMPTY));
    Assert.assertEquals(new Column("a").hashCode(), new Column(Bytes.wrap("a"), Bytes.EMPTY, Bytes.EMPTY).hashCode());
    
    col = new Column("cf1");
    Assert.assertTrue(col.isFamilySet());
    Assert.assertFalse(col.isQualifierSet());
    Assert.assertFalse(col.isVisibilitySet());
    Assert.assertEquals(Bytes.wrap("cf1"), col.getFamily());
    Assert.assertSame(Bytes.EMPTY, col.getQualifier());
    Assert.assertSame(Bytes.EMPTY, col.getVisibility());
    Assert.assertEquals(new Column("cf1"), col);
    
    col = new Column("cf2", "cq2");
    Assert.assertTrue(col.isFamilySet());
    Assert.assertTrue(col.isQualifierSet());
    Assert.assertFalse(col.isVisibilitySet());
    Assert.assertEquals(Bytes.wrap("cf2"), col.getFamily());
    Assert.assertEquals(Bytes.wrap("cq2"), col.getQualifier());
    Assert.assertSame(Bytes.EMPTY, col.getVisibility());
    Assert.assertEquals(new Column("cf2", "cq2"), col);
    
    col = new Column("cf3", "cq3", "cv3");
    Assert.assertTrue(col.isFamilySet());
    Assert.assertTrue(col.isQualifierSet());
    Assert.assertTrue(col.isVisibilitySet());
    Assert.assertEquals(Bytes.wrap("cf3"), col.getFamily());
    Assert.assertEquals(Bytes.wrap("cq3"), col.getQualifier());
    Assert.assertEquals(Bytes.wrap("cv3"), col.getVisibility());
    Assert.assertEquals(new Column("cf3", "cq3", "cv3"), col);
  }
}
