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

import org.apache.accumulo.core.data.Key;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests SpanUtil Class
 */
public class SpanUtilTest {

  @Test
  public void testToKey() {
    Assert.assertNull(SpanUtil.toKey(null));
    Assert.assertEquals(new Key("row"), SpanUtil.toKey(new RowColumn("row")));
    Assert.assertEquals(new Key("row", "cf"),
        SpanUtil.toKey(new RowColumn("row", new Column("cf"))));
    Assert.assertEquals(new Key("row", "cf", "cq"),
        SpanUtil.toKey(new RowColumn("row", new Column("cf", "cq"))));
    Assert.assertEquals(new Key("row", "cf", "cq", "cv"),
        SpanUtil.toKey(new RowColumn("row", new Column("cf", "cq", "cv"))));
  }

  @Test
  public void testToRowColumn() {
    Assert.assertEquals(RowColumn.EMPTY, SpanUtil.toRowColumn(null));
    Assert.assertEquals(new RowColumn("row"), SpanUtil.toRowColumn(new Key("row")));
    Assert.assertEquals(new RowColumn("row", new Column("cf")),
        SpanUtil.toRowColumn(new Key("row", "cf")));
    Assert.assertEquals(new RowColumn("row", new Column("cf", "cq")),
        SpanUtil.toRowColumn(new Key("row", "cf", "cq")));
    Assert.assertEquals(new RowColumn("row", new Column("cf", "cq", "cv")),
        SpanUtil.toRowColumn(new Key("row", "cf", "cq", "cv")));
  }
}
