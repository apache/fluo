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

package org.apache.fluo.accumulo.values;

import org.apache.fluo.accumulo.util.ByteArrayUtil;

public class DelLockValue {
  private final boolean primary;
  private final boolean rollback;
  private final long txDoneTs;

  public DelLockValue(byte[] data) {
    primary = isPrimary(data);
    rollback = isRollback(data);
    txDoneTs = getTxDoneTimestamp(data);
  }

  public long getCommitTimestamp() {
    return txDoneTs;
  }

  public static long getTxDoneTimestamp(byte[] data) {
    return ByteArrayUtil.decodeLong(data, 1);
  }

  public boolean isPrimary() {
    return primary;
  }

  public static boolean isPrimary(byte[] data) {
    return (data[0] & 0x01) == 1;
  }

  public boolean isRollback() {
    return rollback;
  }

  public static boolean isRollback(byte[] data) {
    return (data[0] & 0x02) == 2;
  }

  public static byte[] encodeCommit(long ts, boolean primary) {
    byte[] ba = new byte[9];
    ba[0] = (byte) (primary ? 1 : 0);
    ByteArrayUtil.encode(ba, 1, ts);
    return ba;
  }

  public static byte[] encodeRollback(boolean primary, boolean rollback) {
    byte[] ba = new byte[9];
    ba[0] = (byte) ((primary ? 1 : 0) | (rollback ? 2 : 0));
    ByteArrayUtil.encode(ba, 1, 0);
    return ba;
  }

  public static byte[] encodeRollback(long ts, boolean primary, boolean rollback) {
    byte[] ba = new byte[9];
    ba[0] = (byte) ((primary ? 1 : 0) | (rollback ? 2 : 0));
    ByteArrayUtil.encode(ba, 1, ts);
    return ba;
  }

  @Override
  public String toString() {
    return (rollback ? "ABORT " : "COMMIT ") + (primary ? "PRIMARY " : "") + txDoneTs;
  }
}
