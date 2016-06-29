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

package org.apache.fluo.api.types;

import org.apache.fluo.api.data.Bytes;

/**
 * Transforms Java primitives to and from bytes using desired encoding
 *
 * @since 1.0.0
 */
public interface Encoder {

  /**
   * Encodes an integer to {@link Bytes}
   */
  Bytes encode(int i);

  /**
   * Encodes a long to {@link Bytes}
   */
  Bytes encode(long l);

  /**
   * Encodes a String to {@link Bytes}
   */
  Bytes encode(String s);

  /**
   * Encodes a float to {@link Bytes}
   */
  Bytes encode(float f);

  /**
   * Encodes a double to {@link Bytes}
   */
  Bytes encode(double d);

  /**
   * Encodes a boolean to {@link Bytes}
   */
  Bytes encode(boolean b);

  /**
   * Decodes an integer from {@link Bytes}
   */
  int decodeInteger(Bytes b);

  /**
   * Decodes a long from {@link Bytes}
   */
  long decodeLong(Bytes b);

  /**
   * Decodes a String from {@link Bytes}
   */
  String decodeString(Bytes b);

  /**
   * Decodes a float from {@link Bytes}
   */
  float decodeFloat(Bytes b);

  /**
   * Decodes a double from {@link Bytes}
   */
  double decodeDouble(Bytes b);

  /**
   * Decodes a boolean from {@link Bytes}
   */
  boolean decodeBoolean(Bytes b);
}
