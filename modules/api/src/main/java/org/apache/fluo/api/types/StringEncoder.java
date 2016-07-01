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
 * Transforms Java primitives to and from bytes using a String encoding
 *
 * @since 1.0.0
 */
public class StringEncoder implements Encoder {

  @Override
  public Bytes encode(int i) {
    return encode(Integer.toString(i));
  }

  @Override
  public Bytes encode(long l) {
    return encode(Long.toString(l));
  }

  @Override
  public Bytes encode(String s) {
    return Bytes.of(s);
  }

  @Override
  public Bytes encode(float f) {
    return encode(Float.toString(f));
  }

  @Override
  public Bytes encode(double d) {
    return encode(Double.toString(d));
  }

  @Override
  public Bytes encode(boolean b) {
    return encode(Boolean.toString(b));
  }

  @Override
  public int decodeInteger(Bytes b) {
    return Integer.parseInt(decodeString(b));
  }

  @Override
  public long decodeLong(Bytes b) {
    return Long.parseLong(decodeString(b));
  }

  @Override
  public String decodeString(Bytes b) {
    return b.toString();
  }

  @Override
  public float decodeFloat(Bytes b) {
    return Float.parseFloat(decodeString(b));
  }

  @Override
  public double decodeDouble(Bytes b) {
    return Double.parseDouble(decodeString(b));
  }

  @Override
  public boolean decodeBoolean(Bytes b) {
    return Boolean.parseBoolean(decodeString(b));
  }
}
