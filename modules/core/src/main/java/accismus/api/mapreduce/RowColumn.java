package accismus.api.mapreduce;

import org.apache.accumulo.core.data.ByteSequence;

import accismus.api.Column;

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
