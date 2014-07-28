package io.fluo.api.mapreduce;

import io.fluo.api.Bytes;
import io.fluo.api.Column;

public class RowColumn {
  private Bytes row;
  private Column col;

  public RowColumn(Bytes row, Column col) {
    this.row = row;
    this.col = col;
  }

  public Bytes getRow() {
    return row;
  }

  public Column getColumn() {
    return col;
  }
}
