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
package io.fluo.core.impl;

import java.util.Map.Entry;

import io.fluo.accumulo.util.ColumnConstants;

import io.fluo.accumulo.values.LockValue;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Encapsulates the information needed to indentify a transactions primary row and column.
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

  public PrimaryRowColumn(Entry<Key,Value> lock) {
    LockValue lv = new LockValue(lock.getValue().get());

    this.prow = lv.getPrimaryRow();
    this.pcol = lv.getPrimaryColumn();
    this.startTs = lock.getKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;
  }

  public int weight() {
    return 32 + prow.length() + pcol.getFamily().length() + pcol.getQualifier().length() + pcol.getVisibility().length();
  }

  public boolean equals(Object o) {
    if (o instanceof PrimaryRowColumn) {
      PrimaryRowColumn ock = (PrimaryRowColumn) o;
      return prow.equals(ock.prow) && pcol.equals(ock.pcol) && startTs == ock.startTs;
    }

    return false;
  }

  public int hashCode() {
    return prow.hashCode() + pcol.hashCode() + Long.valueOf(startTs).hashCode();
  }

  public String toString() {
    return prow + " " + pcol + " " + startTs;
  }
}