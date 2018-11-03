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

import org.apache.fluo.api.data.Bytes;

/**
 * Constants used extract data from columns
 */
public class ColumnConstants {
  public static final long PREFIX_MASK = -1L << (64 - ColumnType.BITS);
  public static final long TIMESTAMP_MASK = -1L >>> ColumnType.BITS;
  public static final Bytes NOTIFY_CF = Bytes.of("ntfy");
  public static final String NOTIFY_LOCALITY_GROUP_NAME = "notify";
  public static final Bytes GC_CF = Bytes.of("gc");

  private ColumnConstants() {}
}
