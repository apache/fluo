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

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.data.Key;

/**
 * Abstracts how the Fluo column type is encoded in Accumulo timestamps.
 */
public enum ColumnType {

  TX_DONE, WRITE, DEL_LOCK, RLOCK, LOCK, ACK, DATA;

  private long prefix;

  /**
   * @return The first possible timestamp in sorted order.
   */
  public long first() {
    return prefix | ColumnConstants.TIMESTAMP_MASK;
  }

  /**
   * @return The timestamp with this column type encoded into the high order bits.
   */
  public long enode(long timestamp) {
    Preconditions.checkArgument((timestamp >>> (64 - BITS)) == 0);
    return prefix | timestamp;
  }

  // The number of leftmost bits in in the timestamp reserved for encoding the column type
  static final int BITS = 3;
  private static final byte TX_DONE_PREFIX = 0x03;
  private static final byte WRITE_PREFIX = 0x02;
  private static final byte DEL_LOCK_PREFIX = 0x01;
  private static final byte RLOCK_PREFIX = 0x00;
  private static final byte LOCK_PREFIX = 0x07;
  private static final byte ACK_PREFIX = 0x06;
  private static final byte DATA_PREFIX = 0x05;

  static {
    TX_DONE.prefix = (long) TX_DONE_PREFIX << (64 - BITS);
    WRITE.prefix = (long) WRITE_PREFIX << (64 - BITS);
    DEL_LOCK.prefix = (long) DEL_LOCK_PREFIX << (64 - BITS);
    RLOCK.prefix = (long) RLOCK_PREFIX << (64 - BITS);
    LOCK.prefix = (long) LOCK_PREFIX << (64 - BITS);
    ACK.prefix = (long) ACK_PREFIX << (64 - BITS);
    DATA.prefix = (long) DATA_PREFIX << (64 - BITS);
  }

  public static ColumnType from(Key k) {
    return from(k.getTimestamp());
  }

  public static ColumnType from(long timestamp) {
    byte prefix = (byte) (timestamp >>> (64 - BITS));
    switch (prefix) {
      case TX_DONE_PREFIX:
        return TX_DONE;
      case WRITE_PREFIX:
        return WRITE;
      case DEL_LOCK_PREFIX:
        return DEL_LOCK;
      case RLOCK_PREFIX:
        return RLOCK;
      case LOCK_PREFIX:
        return LOCK;
      case ACK_PREFIX:
        return ACK;
      case DATA_PREFIX:
        return DATA;
      default:
        throw new IllegalArgumentException("Unknown prefix : " + Integer.toHexString(prefix));
    }
  }
}
