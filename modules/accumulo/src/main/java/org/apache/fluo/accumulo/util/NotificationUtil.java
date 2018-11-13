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

import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.fluo.accumulo.iterators.NotificationIterator;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

public class NotificationUtil {

  private static final long DEL_MASK = 0x0000000000000001L;

  public static boolean isDelete(Key k) {
    return isDelete(k.getTimestamp());
  }

  public static boolean isDelete(long ts) {
    return (ts & NotificationUtil.DEL_MASK) == NotificationUtil.DEL_MASK;
  }

  public static boolean isNtfy(Key key) {
    return key.getColumnFamilyData().equals(NotificationIterator.NTFY_CF);
  }

  public static long encodeTs(long ts, boolean isDelete) {
    return ts << 1 | (isDelete ? 1 : 0);
  }

  public static long decodeTs(Key k) {
    return decodeTs(k.getTimestamp());
  }

  public static long decodeTs(long ts) {
    return ts >> 1;
  }

  public static byte[] encodeCol(Column c) {
    return ByteArrayUtil.concat(c.getFamily(), c.getQualifier());
  }

  public static Column decodeCol(Key k) {
    return decodeCol(k.getColumnQualifierData().toArray());
  }

  public static Column decodeCol(byte[] cq) {
    List<Bytes> ca = ByteArrayUtil.split(cq);
    return new Column(ca.get(0), ca.get(1));
  }
}
