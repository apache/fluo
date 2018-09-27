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

package org.apache.fluo.integration;

import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

public class TestUtil {

  private TestUtil() {}

  private static final Bytes ZERO = Bytes.of("0");

  public static void increment(TransactionBase tx, Bytes row, Column col, int val) {
    int prev = 0;
    String prevStr = tx.get(row, col, ZERO).toString();
    prev = Integer.parseInt(prevStr);
    tx.set(row, col, Bytes.of(prev + val + ""));
  }

  public static void increment(TransactionBase tx, String row, Column col, int val) {
    int prev = 0;
    String prevStr = tx.gets(row, col, "0");
    prev = Integer.parseInt(prevStr);
    tx.set(row, col, prev + val + "");
  }

  public static int getOrDefault(SnapshotBase snap, String row, Column col, int defaultVal) {
    String val = snap.gets(row, col);
    if (val == null) {
      return defaultVal;
    }
    return Integer.parseInt(val);
  }
}
