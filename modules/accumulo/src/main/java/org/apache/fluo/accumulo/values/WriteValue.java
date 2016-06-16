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

public class WriteValue {
  private final boolean primary;
  private final boolean delete;
  private final long ts;

  public WriteValue(byte[] data) {
    primary = isPrimary(data);
    delete = isDelete(data);
    ts = getTimestamp(data);
  }

  public boolean isPrimary() {
    return primary;
  }

  public static boolean isPrimary(byte[] data) {
    return (data[0] & 0x01) == 1;
  }

  public boolean isDelete() {
    return delete;
  }

  public static boolean isDelete(byte[] data) {
    return (data[0] & 0x02) == 2;
  }

  public long getTimestamp() {
    return ts;
  }

  public static long getTimestamp(byte[] data) {
    return ByteArrayUtil.decodeLong(data, 1);
  }

  @Override
  public String toString() {
    return ts + (delete ? " DELETE" : "") + " " + (primary ? "PRIMARY" : "");
  }

  public static byte[] encode(long ts, boolean primary, boolean delete) {
    byte[] ba = new byte[9];
    ba[0] = (byte) ((primary ? 1 : 0) | (delete ? 2 : 0));
    ByteArrayUtil.encode(ba, 1, ts);
    return ba;
  }
}
