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

import java.nio.charset.StandardCharsets;

/**
 * Utilities for working with Java Long
 */
public class LongUtil {

  private LongUtil() {}

  /**
   * Converts given Long to String using max radix
   */
  public static String toMaxRadixString(Long value) {
    return Long.toString(value, Character.MAX_RADIX);
  }

  /**
   * Converts from given String to Long using max radix
   */
  public static Long fromMaxRadixString(String value) {
    return Long.parseLong(value, Character.MAX_RADIX);
  }

  /**
   * Converts given Long to max radix byte array
   */
  public static byte[] toMaxRadixByteArray(Long value) {
    return toMaxRadixString(value).getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Converts given max radix byte array to Long
   */
  public static Long fromMaxRadixByteArray(byte[] value) {
    return fromMaxRadixString(new String(value, StandardCharsets.UTF_8));
  }

  /**
   * Convert given Long to byte array
   */
  public static byte[] toByteArray(Long value) {
    return value.toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Convert given byte array to Long
   */
  public static Long fromByteArray(byte[] value) {
    return Long.parseLong(new String(value, StandardCharsets.UTF_8));
  }
}
