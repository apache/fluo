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

public class DelReadLockValue {

  long commitTs = -1;
  boolean rollback;

  public DelReadLockValue(byte[] value) {
    this.rollback = isRollback(value);
    if (!this.rollback) {
      this.commitTs = getCommitTimestamp(value);
    }
  }


  public static byte[] encodeRollback() {
    byte[] ba = new byte[1];
    ba[0] = (byte) 1;
    return ba;
  }

  public static byte[] encodeCommit(long commitTs) {
    byte[] ba = new byte[9];
    ba[0] = (byte) 0;
    ByteArrayUtil.encode(ba, 1, commitTs);
    return ba;
  }

  public static boolean isRollback(byte[] data) {
    return (data[0] & 0x01) == 1;
  }

  public static long getCommitTimestamp(byte[] data) {
    return ByteArrayUtil.decodeLong(data, 1);
  }

  @Override
  public String toString() {
    return commitTs + (rollback ? " ABORT" : " COMMIT");
  }
}
