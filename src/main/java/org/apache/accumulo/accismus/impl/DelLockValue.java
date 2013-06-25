/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.accismus.impl;

public class DelLockValue {
  private boolean isRollback;
  private long lockTime;
  
  
  public DelLockValue(byte[] data) {
    isRollback = data[0] == 1;
    lockTime = ByteUtil.decodeLong(data, 1);
  }
  
  public long getLockTime() {
    return lockTime;
  }
  
  public boolean isRollback() {
    return isRollback;
  }
  
  public static byte[] encode(long ts, boolean isRollback) {
    byte ba[] = new byte[9];
    ba[0] = (byte) (isRollback ? 1 : 0);
    ByteUtil.encode(ba, 1, ts);
    return ba;
  }
  
  public String toString() {
    return (isRollback ? "ABORT " : "COMMIT ") + lockTime;
  }
}