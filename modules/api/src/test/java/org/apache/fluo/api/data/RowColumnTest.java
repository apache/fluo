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
import org.apache.fluo.api.data.RowColumn;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link RowColumn}
 */
public class RowColumnTest {

  @Test
  public void testBasic() {

    RowColumn rc = new RowColumn();
    Assert.assertEquals(RowColumn.EMPTY, rc);
    Assert.assertEquals(Bytes.EMPTY, rc.getRow());
    Assert.assertEquals("", rc.getsRow());
    Assert.assertEquals(Column.EMPTY, rc.getColumn());
    Assert.assertEquals("   ", rc.toString());
    Assert.assertNotEquals(RowColumn.EMPTY, Column.EMPTY);

    rc = new RowColumn(Bytes.of("r1"));
    Assert.assertEquals(Bytes.of("r1"), rc.getRow());
    Assert.assertEquals("r1", rc.getsRow());
    Assert.assertEquals(Column.EMPTY, rc.getColumn());
    Assert.assertEquals(new RowColumn("r1"), rc);
    Assert.assertEquals("r1   ", rc.toString());

    rc = new RowColumn("r2", new Column("cf2"));
    Assert.assertEquals(Bytes.of("r2"), rc.getRow());
    Assert.assertEquals("r2", rc.getsRow());
    Assert.assertEquals(new Column("cf2"), rc.getColumn());
    Assert.assertEquals(new RowColumn(Bytes.of("r2"), new Column("cf2")), rc);
    Assert.assertEquals("r2 cf2  ", rc.toString());
    Assert.assertEquals(123316141, rc.hashCode());
  }

  @Test
  public void testFollowing() {
    byte[] fdata = new String("data1").getBytes();
    fdata[4] = (byte) 0x00;
    Bytes fb = Bytes.of(fdata);

    Assert.assertEquals(RowColumn.EMPTY, new RowColumn().following());
    Assert.assertEquals(new RowColumn(fb), new RowColumn("data").following());
    Assert.assertEquals(new RowColumn("row", new Column(fb)),
        new RowColumn("row", new Column("data")).following());
    Assert.assertEquals(new RowColumn("row", new Column(Bytes.of("cf"), fb)),
        new RowColumn("row", new Column("cf", "data")).following());
    Assert.assertEquals(new RowColumn("row", new Column(Bytes.of("cf"), Bytes.of("cq"), fb)),
        new RowColumn("row", new Column("cf", "cq", "data")).following());
  }

  @Test
  public void testCompare() {
    RowColumn rc1 = new RowColumn("a", new Column("b"));
    RowColumn rc2 = new RowColumn("b");
    RowColumn rc3 = new RowColumn("a", new Column("c"));
    RowColumn rc4 = new RowColumn("a", new Column("c", "d"));
    Assert.assertEquals(-1, rc1.compareTo(rc2));
    Assert.assertEquals(1, rc2.compareTo(rc1));
    Assert.assertEquals(-1, rc1.compareTo(rc3));
    Assert.assertEquals(-1, rc3.compareTo(rc2));
    Assert.assertEquals(0, rc3.compareTo(rc3));
    Assert.assertEquals(1, rc2.compareTo(RowColumn.EMPTY));
    Assert.assertEquals(-1, rc3.compareTo(rc4));
  }
}
