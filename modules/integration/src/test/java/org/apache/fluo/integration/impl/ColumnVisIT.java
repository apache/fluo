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

import java.util.Arrays;

import com.google.common.collect.Sets;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.junit.Test;

public class ColumnVisIT extends ITBaseImpl {

  @Test(expected = Exception.class)
  public void testFailFastSet() {
    TestTransaction tx1 = new TestTransaction(env);

    // expect set w/ bad col vis to fail fast
    tx1.set("r", new Column("f", "q", "A&"), "v");
  }

  @Test(expected = Exception.class)
  public void testFailFastDelete() {
    TestTransaction tx1 = new TestTransaction(env);

    // expect delete w/ bad col vis to fail fast
    tx1.delete("r", new Column("f", "q", "A&"));
  }

  @Test(expected = Exception.class)
  public void testFailFastWeakNotify() {
    TestTransaction tx1 = new TestTransaction(env);

    // expect weaknotify w/ bad col vis to fail fast
    tx1.setWeakNotification("r", new Column("f", "q", "A&"));
  }

  @Test(expected = Exception.class)
  public void testFailFastGet() {
    TestTransaction tx1 = new TestTransaction(env);

    // expect get w/ bad col vis to fail fast
    tx1.gets("r", new Column("f", "q", "A&"));
  }

  @Test(expected = Exception.class)
  public void testFailFastGetCols() {
    TestTransaction tx1 = new TestTransaction(env);

    Column col1 = new Column("f", "q", "A&");
    Column col2 = new Column("f", "q", "C|");

    // expect get cols w/ bad col vis to fail fast
    tx1.gets("r", Sets.newHashSet(col1, col2)).size();
  }

  @Test(expected = Exception.class)
  public void testFailFastGetRowsCols() {
    TestTransaction tx1 = new TestTransaction(env);

    Column col1 = new Column("f", "q", "A&");
    Column col2 = new Column("f", "q", "C|");

    // expect get rows cols w/ bad col vis to fail fast
    tx1.gets(Arrays.asList("r1", "r2"), Sets.newHashSet(col1, col2)).size();
  }
}
