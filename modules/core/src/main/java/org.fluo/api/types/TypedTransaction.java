package org.fluo.api.types;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;

import org.fluo.api.Column;
import org.fluo.api.Transaction;
import org.fluo.api.types.TypeLayer.RowAction;
import org.fluo.api.types.TypeLayer.RowColumnBuilder;

public class TypedTransaction extends TypedSnapshot implements Transaction {

  private Transaction tx;
  private Encoder encoder;
  private TypeLayer tl;

  private class MutateKeyBuilder extends RowColumnBuilder<Mutator,VisibilityMutator> {

    private ByteSequence row;
    private ByteSequence cf;

    @Override
    void setRow(ByteSequence r) {
      this.row = r;
    }

    @Override
    void setFamily(ByteSequence f) {
      this.cf = f;
    }

    @Override
    VisibilityMutator setQualifier(ByteSequence q) {
      return new VisibilityMutator(row, new Column(cf, q));
    }

    @Override
    Mutator setColumn(Column c) {
      return new Mutator(row, c);
    }

  }

  public class Mutator {

    private ByteSequence row;
    private Column col;
    private boolean set = false;

    void checkNotSet() {
      if (set)
        throw new IllegalStateException("Already set value");
    }

    Mutator(ByteSequence row, Column column) {
      this.row = row;
      this.col = column;
    }

    public void set(String s) {
      checkNotSet();
      tx.set(row, col, encoder.encode(s));
      set = true;
    }

    public void set(int i) {
      checkNotSet();
      tx.set(row, col, encoder.encode(i));
      set = true;
    }

    public void set(long l) {
      checkNotSet();
      tx.set(row, col, encoder.encode(l));
      set = true;
    }

    public void increment(int i) throws Exception {
      checkNotSet();
      ByteSequence val = tx.get(row, col);
      int v = 0;
      if (val != null)
        v = encoder.decodeInteger(val);
      tx.set(row, col, encoder.encode(v + i));
    }

    public void increment(long l) throws Exception {
      checkNotSet();
      ByteSequence val = tx.get(row, col);
      long v = 0;
      if (val != null)
        v = encoder.decodeLong(val);
      tx.set(row, col, encoder.encode(v + l));
    }

    public void set(byte[] ba) {
      checkNotSet();
      tx.set(row, col, new ArrayByteSequence(ba));
      set = true;
    }

    /**
     * Set an empty value
     */
    public void set() {
      checkNotSet();
      tx.set(row, col, new ArrayByteSequence(new byte[0]));
      set = true;
    }

    public void delete() {
      checkNotSet();
      tx.delete(row, col);
      set = true;
    }

    public void weaklyNotify() {
      checkNotSet();
      tx.setWeakNotification(row, col);
      set = true;
    }

  }

  public class VisibilityMutator extends Mutator {

    VisibilityMutator(ByteSequence row, Column column) {
      super(row, column);
    }

    public Mutator vis(ColumnVisibility cv) {
      checkNotSet();
      super.col.setVisibility(cv);
      super.set = true;
      return new Mutator(super.row, super.col);
    }
  }


  // TODO make private.. test depend on it
  protected TypedTransaction(Transaction tx, Encoder encoder, TypeLayer tl) {
    super(tx, encoder, tl);
    this.tx = tx;
    this.encoder = encoder;
    this.tl = tl;
  }

  public RowAction<Mutator,VisibilityMutator,MutateKeyBuilder> mutate() {
    return tl.new RowAction<Mutator,VisibilityMutator,MutateKeyBuilder>(new MutateKeyBuilder());
  }

  @Override
  public void set(ByteSequence row, Column col, ByteSequence value) {
    tx.set(row, col, value);
  }

  @Override
  public void setWeakNotification(ByteSequence row, Column col) {
    tx.setWeakNotification(row, col);
  }

  @Override
  public void delete(ByteSequence row, Column col) {
    tx.delete(row, col);
  }
}
