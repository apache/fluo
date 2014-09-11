/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fluo.core.util;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

public class Flutation extends Mutation {
  public Flutation(Bytes row) {
    super(ByteUtil.toText(row));
  }

  public void put(Column col, long ts, byte[] val) {
    put(this, col, ts, val);
  }

  public static void put(Mutation m, Column col, long ts, byte[] val) {
    m.put(ByteUtil.toText(col.getFamily()), ByteUtil.toText(col.getQualifier()), col.getVisibilityParsed(), ts, new Value(val));
  }
}
