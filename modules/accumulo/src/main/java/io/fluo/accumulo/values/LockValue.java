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
package io.fluo.accumulo.values;

import java.util.List;

import io.fluo.accumulo.util.ByteArrayUtil;
import io.fluo.accumulo.util.LongUtil;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;

/**
 * 
 */
public class LockValue {

  private Bytes prow;
  private Column pcol;
  private boolean isWrite;
  private boolean isDelete;
  private boolean isTrigger;
  private Long transactor;

  public LockValue(byte[] enc) {
    List<Bytes> fields = Bytes.split(Bytes.wrap(enc));
    
    if (fields.size() != 6)
      throw new IllegalArgumentException("more fields than expected");
    
    this.prow = fields.get(0);
    this.pcol = new Column(fields.get(1), fields.get(2)).setVisibility(fields.get(3));
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

  public static byte[] encode(Bytes prow, Column pcol, boolean isWrite, boolean isDelete, boolean isTrigger, Long transactor) {
    byte bools[] = new byte[1];
    bools[0] = 0;
    if (isWrite)
      bools[0] = 0x1;
    if (isDelete)
      bools[0] |= 0x2;
    if (isTrigger)
      bools[0] |= 0x4;
    return Bytes.concat(prow, pcol.getFamily(), pcol.getQualifier(), pcol.getVisibility(), 
        Bytes.wrap(bools), Bytes.wrap(ByteArrayUtil.encode(transactor))).toArray();
  }
  
  @Override
  public String toString() {
    return prow + " " + pcol + " " + (isWrite ? "WRITE" : "NOT_WRITE") + " " + (isDelete ? "DELETE" : "NOT_DELETE") 
        + " " + (isTrigger ? "TRIGGER" : "NOT_TRIGGER") + " " + LongUtil.toMaxRadixString(transactor);
  }
}
