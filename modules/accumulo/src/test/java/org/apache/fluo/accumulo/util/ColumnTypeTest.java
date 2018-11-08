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

package org.apache.fluo.accumulo.util;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.apache.accumulo.core.data.Key;
import org.junit.Test;

import static org.apache.fluo.accumulo.util.ColumnType.ACK;
import static org.apache.fluo.accumulo.util.ColumnType.DATA;
import static org.apache.fluo.accumulo.util.ColumnType.DEL_LOCK;
import static org.apache.fluo.accumulo.util.ColumnType.LOCK;
import static org.apache.fluo.accumulo.util.ColumnType.RLOCK;
import static org.apache.fluo.accumulo.util.ColumnType.TX_DONE;
import static org.apache.fluo.accumulo.util.ColumnType.WRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ColumnTypeTest {

  private static final long TIMESTAMP_MASK = 0x1fffffffffffffffL;


  // map of expected prefixes for a column type
  private static final Map<Long, ColumnType> EPM;

  static {
    Builder<Long, ColumnType> builder = ImmutableMap.builder();
    builder.put(0x6000000000000000L, TX_DONE);
    builder.put(0x4000000000000000L, WRITE);
    builder.put(0x2000000000000000L, DEL_LOCK);
    builder.put(0x0000000000000000L, RLOCK);
    builder.put(0xe000000000000000L, LOCK);
    builder.put(0xc000000000000000L, ACK);
    builder.put(0xa000000000000000L, DATA);
    EPM = builder.build();
  }

  @Test
  public void testPrefix() {
    for (long l : new long[] {0, 2, 13, 19 * 19L, 1L << 50, 1L << 50 + 1L << 48}) {
      EPM.forEach((prefix, colType) -> assertEquals(prefix | l, colType.encode(l)));
    }
  }

  @Test
  public void testFirst() {
    EPM.forEach((prefix, colType) -> assertEquals(prefix | TIMESTAMP_MASK, colType.first()));
    for (long l : new long[] {0, 2, 13, 19 * 19L, 1L << 50, 1L << 50 + 1L << 48}) {
      EPM.forEach((prefix, colType) -> {
        Key k1 = new Key("r", "f", "q");
        k1.setTimestamp(prefix | l);
        Key k2 = new Key("r", "f", "q");
        k2.setTimestamp(colType.first());
        assertTrue(k1.compareTo(k2) > 0);
      });
    }
  }

  @Test
  public void testFrom() {
    for (long l : new long[] {0, 2, 13, 19 * 19L, 1L << 50, 1L << 50 + 1L << 48}) {
      EPM.forEach((prefix, colType) -> {
        assertEquals(ColumnType.from(prefix | l), colType);
        Key k = new Key("r", "f", "q");
        k.setTimestamp(prefix | l);
        assertEquals(ColumnType.from(k), colType);
      });
    }
  }

  @Test
  public void testCoverage() {
    EnumSet<ColumnType> expected = EnumSet.allOf(ColumnType.class);
    HashSet<ColumnType> actual = new HashSet<>(EPM.values());
    assertEquals(expected, actual);
  }
}
