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

package org.apache.fluo.core.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.fluo.api.data.Bytes;

public class CachedBytesConverter implements Function<ByteSequence, Bytes> {

  private Map<ByteSequence, Bytes> bs2bCache = new HashMap<>();

  public CachedBytesConverter(Collection<Bytes> bytesCollection) {
    for (Bytes bytes : bytesCollection) {
      bs2bCache.put(ByteUtil.toByteSequence(bytes), bytes);
    }
  }

  @Override
  public Bytes apply(ByteSequence bs) {
    if (bs.length() == 0) {
      return Bytes.EMPTY;
    }

    Bytes b = bs2bCache.get(bs);
    if (b == null) {
      return ByteUtil.toBytes(bs);
    }
    return b;
  }
}
