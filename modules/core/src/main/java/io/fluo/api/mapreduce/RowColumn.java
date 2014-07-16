package io.fluo.api.mapreduce;

import io.fluo.api.Column;
import org.apache.accumulo.core.data.ByteSequence;

public class RowColumn {
  private ByteSequence row;
  private Column col;

  public RowColumn(ByteSequence row, Column col) {
    this.row = row;
    this.col = col;
  }

  public ByteSequence getRow() {
    return row;
  }

  public Column getColumn() {
    return col;
  }
}
