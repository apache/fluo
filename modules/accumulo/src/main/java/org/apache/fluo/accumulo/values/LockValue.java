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

/**
 *
 */
public class LockValue {

  private final Bytes prow;
  private final Column pcol;
  private final boolean isWrite;
  private final boolean isDelete;
  private final boolean isTrigger;
  private final Long transactor;

  public LockValue(byte[] enc) {
    List<Bytes> fields = ByteArrayUtil.split(enc);

    if (fields.size() != 6) {
      throw new IllegalArgumentException("more fields than expected");
    }

    this.prow = fields.get(0);
    this.pcol = new Column(fields.get(1), fields.get(2), fields.get(3));
    this.isWrite = (fields.get(4).byteAt(0) & 0x1) == 0x1;
    this.isDelete = (fields.get(4).byteAt(0) & 0x2) == 0x2;
    this.isTrigger = (fields.get(4).byteAt(0) & 0x4) == 0x4;
    this.transactor = ByteArrayUtil.decodeLong(fields.get(5).toArray());
  }

  public Bytes getPrimaryRow() {
    return prow;
  }

  public Column getPrimaryColumn() {
    return pcol;
  }

  public boolean isWrite() {
    return isWrite;
  }

  public boolean isDelete() {
    return isDelete;
  }

  public boolean isTrigger() {
    return isTrigger;
  }

  public Long getTransactor() {
    return transactor;
  }

  public static byte[] encode(Bytes prow, Column pcol, boolean isWrite, boolean isDelete,
      boolean isTrigger, Long transactor) {
    byte[] bools = new byte[1];
    bools[0] = 0;
    if (isWrite) {
      bools[0] = 0x1;
    }
    if (isDelete) {
      bools[0] |= 0x2;
    }
    if (isTrigger) {
      bools[0] |= 0x4;
    }
    return ByteArrayUtil.concat(prow, pcol.getFamily(), pcol.getQualifier(), pcol.getVisibility(),
        Bytes.of(bools), Bytes.of(ByteArrayUtil.encode(transactor)));
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
    sb.append(isWrite ? "WRITE" : "NOT_WRITE");
    sb.append(" ");
    sb.append(isDelete ? "DELETE" : "NOT_DELETE");
    sb.append(" ");
    sb.append(isTrigger ? "TRIGGER" : "NOT_TRIGGER");
    sb.append(" ");
    sb.append(LongUtil.toMaxRadixString(transactor));

    return sb.toString();
  }
}
