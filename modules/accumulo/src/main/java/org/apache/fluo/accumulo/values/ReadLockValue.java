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

import java.util.List;

import org.apache.fluo.accumulo.util.ByteArrayUtil;
import org.apache.fluo.accumulo.util.LongUtil;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

import static org.apache.fluo.accumulo.format.FluoFormatter.encNonAscii;

public class ReadLockValue {
  private final Bytes prow;
  private final Column pcol;
  private final Long transactor;

  public ReadLockValue(byte[] enc) {
    List<Bytes> fields = ByteArrayUtil.split(enc);

    if (fields.size() != 5) {
      throw new IllegalArgumentException("more fields than expected");
    }

    this.prow = fields.get(0);
    this.pcol = new Column(fields.get(1), fields.get(2), fields.get(3));
    this.transactor = ByteArrayUtil.decodeLong(fields.get(4).toArray());
  }

  public Bytes getPrimaryRow() {
    return prow;
  }

  public Column getPrimaryColumn() {
    return pcol;
  }

  public Long getTransactor() {
    return transactor;
  }

  public static byte[] encode(Bytes prow, Column pcol, Long transactor) {
    return ByteArrayUtil.concat(prow, pcol.getFamily(), pcol.getQualifier(), pcol.getVisibility(),
        Bytes.of(ByteArrayUtil.encode(transactor)));
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    encNonAscii(sb, prow);
    sb.append(" ");
    encNonAscii(sb, pcol.getFamily());
    sb.append(" ");
    encNonAscii(sb, pcol.getQualifier());
    sb.append(" ");
    encNonAscii(sb, pcol.getVisibility());
    sb.append(" ");
    sb.append(LongUtil.toMaxRadixString(transactor));

    return sb.toString();
  }
}
