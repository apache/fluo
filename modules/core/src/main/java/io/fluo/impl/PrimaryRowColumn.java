package io.fluo.impl;

import io.fluo.api.Bytes;
import io.fluo.api.Column;

import java.util.Map.Entry;

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
    this.startTs = lock.getKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
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