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

package org.apache.fluo.core.impl;

import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.values.LockValue;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.core.util.Hex;

/**
 * Encapsulates the information needed to identify a transactions primary row and column.
 */
class PrimaryRowColumn {

  Bytes prow;
  Column pcol;
  long startTs;

  public PrimaryRowColumn(Bytes prow, Column pcol, long startTs) {
    this.prow = prow;
    this.pcol = pcol;
    this.startTs = startTs;
  }

  public PrimaryRowColumn(Entry<Key, Value> lock) {
    LockValue lv = new LockValue(lock.getValue().get());

    this.prow = lv.getPrimaryRow();
    this.pcol = lv.getPrimaryColumn();
    this.startTs = lock.getKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;
  }

  public int weight() {
    return 32 + prow.length() + pcol.getFamily().length() + pcol.getQualifier().length()
        + pcol.getVisibility().length();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PrimaryRowColumn) {
      PrimaryRowColumn ock = (PrimaryRowColumn) o;
      return prow.equals(ock.prow) && pcol.equals(ock.pcol) && startTs == ock.startTs;
    }

    return false;
  }

  @Override
  public int hashCode() {
    return prow.hashCode() + pcol.hashCode() + Long.valueOf(startTs).hashCode();
  }

  @Override
  public String toString() {
    return Hex.encNonAscii(prow) + " " + Hex.encNonAscii(pcol) + " " + startTs;
  }
}
