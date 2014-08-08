package io.fluo.api.mapreduce;

import com.google.common.base.Preconditions;

import io.fluo.api.Bytes;
import io.fluo.api.Column;

/**
 * Represents a Row & Column in Fluo. 
 * Similiar to an Accumulo Key.
 */
public class RowColumn {
  
  public static RowColumn EMPTY = new RowColumn();
  
  private Bytes row = Bytes.EMPTY;
  private Column col = Column.EMPTY;
  
  /** 
   * Constructs a RowColumn with row set to Bytes.EMPTY
   * and column set to Column.EMPTY 
   */
  public RowColumn() {}
  
  /**
   * Constructs a RowColumn with only a row.  
   * Column will be set to Column.EMPTY
   * 
   * @param row Bytes Row
   */
  public RowColumn(Bytes row) {
    Preconditions.checkNotNull(row, "Row must not be null");
    this.row = row;
  }
  
  /**
   * Constructs a RowColumn with only a row.
   * Column will be set to Column.EMPTY
   * 
   * @param row (will be UTF-8 encoded)
   */
  public RowColumn(String row) {
    this(row == null ? null : Bytes.wrap(row));
  }
  
  /**
   * Constructs a RowColumn
   * 
   * @param row Bytes Row
   * @param col Column
   */
  public RowColumn(Bytes row, Column col) {
    Preconditions.checkNotNull(row, "Row must not be null");
    Preconditions.checkNotNull(col, "Col must not be null");
    this.row = row;
    this.col = col;
  }
  
  /**
   * Constructs a RowColumn
   * 
   * @param row Row String (will be UTF-8 encoded)
   * @param col Column
   */
  public RowColumn(String row, Column col) {
    this(row == null ? null : Bytes.wrap(row), col);
  }

  /**
   * Retrieves Row in RowColumn
   * 
   * @return Row
   */
  public Bytes getRow() {
    return row;
  }

  /**
   * Retrieves Column in RowColumn
   * 
   * @return Column
   */
  public Column getColumn() {
    return col;
  }
  
  @Override
  public String toString() {
    return row + " " + col;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof RowColumn) { 
      RowColumn other = (RowColumn) o;
      return row.equals(other.row) && col.equals(other.col);
    }
    return false;
  }
  
  /**
   * Returns a RowColumn following the current one
   * 
   * @return RowColumn following this one
   */
  public RowColumn following() {
    if (row.equals(Bytes.EMPTY)) {
      return RowColumn.EMPTY;
    } else if (col.equals(Column.EMPTY)) {
      return new RowColumn(followingBytes(row));
    } else if (col.getQualifier().equals(Bytes.EMPTY)) {
      return new RowColumn(row, new Column(followingBytes(col.getFamily())));
    } else if (col.getVisibility().equals(Bytes.EMPTY)) {
      return new RowColumn(row, new Column(col.getFamily(), followingBytes(col.getQualifier())));
    } else {
      return new RowColumn(row, new Column(col.getFamily(), col.getQualifier(), followingBytes(col.getVisibility())));
    }
  }
  
  private byte[] followingArray(byte ba[]) {
    byte[] fba = new byte[ba.length + 1];
    System.arraycopy(ba, 0, fba, 0, ba.length);
    fba[ba.length] = (byte) 0x00;
    return fba;
  }
  
  private Bytes followingBytes(Bytes b) {
    return Bytes.wrap(followingArray(b.toArray()));
  }
}
