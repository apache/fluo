/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.fluo.accumulo.util;

import io.fluo.api.data.Bytes;

/**
 * Constants used extract data from columns
 */
public class ColumnConstants {

  public static final long PREFIX_MASK = 0xe000000000000000l;
  public static final long TX_DONE_PREFIX = 0x6000000000000000l;
  public static final long WRITE_PREFIX = 0x4000000000000000l;
  public static final long DEL_LOCK_PREFIX = 0x2000000000000000l;
  public static final long LOCK_PREFIX = 0xe000000000000000l;
  public static final long ACK_PREFIX = 0xc000000000000000l;
  public static final long DATA_PREFIX = 0xa000000000000000l;
  public static final long TIMESTAMP_MASK = 0x1fffffffffffffffl;
  public static final Bytes NOTIFY_CF = Bytes.of("ntfy");

  private ColumnConstants() {}

}
