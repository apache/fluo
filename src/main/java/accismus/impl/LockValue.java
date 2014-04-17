/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package accismus.impl;

import java.util.List;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;

import accismus.api.Column;

/**
 * 
 */
public class LockValue {
  
  private static final ByteSequence TBS = new ArrayByteSequence("T");
  private static final ByteSequence FBS = new ArrayByteSequence("F");
  
  private ByteSequence prow;
  private Column pcol;
  private boolean isWrite;
  private ByteSequence observer;

  public LockValue(byte[] enc) {
    List<ByteSequence> fields = ByteUtil.split(new ArrayByteSequence(enc));
    
    if (fields.size() != 6)
      throw new IllegalArgumentException("more fields than expected");
    
    this.prow = fields.get(0);
    this.pcol = new Column(fields.get(1), fields.get(2)).setVisibility(new ColumnVisibility(fields.get(3).toArray()));
    this.isWrite = fields.get(4).equals(TBS);
    this.observer = fields.get(5);
    
  }
  
  public ByteSequence getPrimaryRow() {
    return prow;
  }
  
  public Column getPrimaryColumn() {
    return pcol;
  }
  
  public boolean isWrite() {
    return isWrite;
  }
  
  public ByteSequence getObserver() {
    return observer;
  }
  public static byte[] encode(ByteSequence prow, Column pcol, boolean isWrite, ByteSequence observer) {
    return ByteUtil.concat(prow, pcol.getFamily(), pcol.getQualifier(), new ArrayByteSequence(pcol.getVisibility().getExpression()), isWrite ? TBS : FBS,
        observer);
  }
  
  public String toString() {
    return prow + " " + pcol + " " + (isWrite ? "WRITE" : "NOT_WRITE") + " " + observer;
  }
}
