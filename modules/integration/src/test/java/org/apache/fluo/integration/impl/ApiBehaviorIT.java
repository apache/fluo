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

package org.apache.fluo.integration.impl;

import java.util.Collections;
import java.util.Set;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Assert;
import org.junit.Test;

public class ApiBehaviorIT extends ITBaseImpl {
  @Test
  public void testGetNonexistant() {

    TestTransaction tx1 = new TestTransaction(env);

    Set<String> rss = Collections.singleton("foo");
    Set<Bytes> rowSet = Collections.singleton(Bytes.of("foo"));
    Set<Column> colSet = Collections.singleton(new Column("a", "b"));

    Assert.assertEquals(0, tx1.get(Bytes.of("foo"), colSet).size());
    Assert.assertEquals(0, tx1.gets("foo", colSet).size());
    Assert.assertEquals(0, tx1.get(rowSet, colSet).size());
    Assert.assertEquals(0, tx1.gets(rss, colSet).size());
  }

  @Test
  public void testEmptyInputs() {
    TestTransaction tx1 = new TestTransaction(env);

    Set<Bytes> rowSet = Collections.singleton(Bytes.of("foo"));
    Set<Column> colSet = Collections.singleton(new Column("a", "b"));

    Set<Bytes> emptyRowSet = Collections.emptySet();
    Set<Column> emptyColSet = Collections.emptySet();
    Set<RowColumn> emptyRowColSet = Collections.emptySet();

    Assert.assertEquals(0, tx1.get(Bytes.of("foo"), emptyColSet).size());
    Assert.assertEquals(0, tx1.get(emptyRowSet, emptyColSet).size());
    Assert.assertEquals(0, tx1.get(emptyRowSet, colSet).size());
    Assert.assertEquals(0, tx1.get(rowSet, emptyColSet).size());
    Assert.assertEquals(0, tx1.get(rowSet, emptyColSet).size());
    Assert.assertEquals(0, tx1.get(emptyRowColSet).size());

    Set<String> erss = Collections.emptySet();
    Set<String> rss = Collections.singleton("foo");

    Assert.assertEquals(0, tx1.gets("foo", emptyColSet).size());
    Assert.assertEquals(0, tx1.gets(erss, emptyColSet).size());
    Assert.assertEquals(0, tx1.gets(erss, colSet).size());
    Assert.assertEquals(0, tx1.gets(rss, emptyColSet).size());
    Assert.assertEquals(0, tx1.gets(rss, emptyColSet).size());
    Assert.assertEquals(0, tx1.gets(emptyRowColSet).size());
  }
}
