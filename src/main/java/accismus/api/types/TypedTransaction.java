package accismus.api.types;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;

import accismus.api.Column;
import accismus.api.Transaction;
import accismus.api.types.TypeLayer.RowAction;
import accismus.api.types.TypeLayer.RowColumnBuilder;

public class TypedTransaction extends TypedSnapshot implements Transaction {

  private Transaction tx;
  private Encoder encoder;
  private TypeLayer tl;

  private class KeyBuilder extends RowColumnBuilder<ValueSetter,VisAction> {

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
    VisAction setQualifier(ByteSequence q) {
      return new VisAction(row, new Column(cf, q));
    }

    @Override
    ValueSetter setColumn(Column c) {
      return new ValueSetter(row, c);
    }

  }

  public class VisAction extends ValueSetter {
    private VisAction(ByteSequence row, Column col) {
      super(row, col);
    }

    public ValueSetter vis(ColumnVisibility cv) {
      checkNotSet();
      super.col.setVisibility(cv);
      super.set = true;
      return new ValueSetter(super.row, super.col);
    }
  }

  public class ValueSetter {
    private ByteSequence row;
    private Column col;
    private boolean set = false;

    void checkNotSet() {
      if (set)
        throw new IllegalStateException("Already set value");
    }

    ValueSetter(ByteSequence row, Column col) {
      this.row = row;
      this.col = col;
    }

    public void val(String s) {
      checkNotSet();
      tx.set(row, col, encoder.encode(s));
      set = true;
    }

    public void val(int i) {
      checkNotSet();
      tx.set(row, col, encoder.encode(i));
      set = true;
    }

    public void val(long l) {
      checkNotSet();
      tx.set(row, col, encoder.encode(l));
      set = true;
    }

    public void val(byte[] ba) {
      checkNotSet();
      tx.set(row, col, new ArrayByteSequence(ba));
      set = true;
    }

    /**
     * Set an empty value
     */
    public void val() {
      checkNotSet();
      tx.set(row, col, new ArrayByteSequence(new byte[0]));
      set = true;
    }
  }

  private class DeleteKeyBuilder extends RowColumnBuilder<Deleted,VisDeleter> {

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
    VisDeleter setQualifier(ByteSequence q) {
      return new VisDeleter(row, cf, q);
    }

    @Override
    Deleted setColumn(Column c) {
      tx.delete(row, c);
      return null;
    }

  }

  /**
   * this return type indicates the row column was set for delete.
   */
  public class Deleted {

  }

  public class VisDeleter {

    private ByteSequence row;
    private ByteSequence cf;
    private ByteSequence cq;

    public VisDeleter(ByteSequence row, ByteSequence cf, ByteSequence q) {
      this.row = row;
      this.cf = cf;
      this.cq = q;
    }

    public Deleted vis(ColumnVisibility cv) {
      tx.delete(row, new Column(cf, cq).setVisibility(cv));
      return null;
    }

  }

  // TODO make private.. test depend on it
  protected TypedTransaction(Transaction tx, Encoder encoder, TypeLayer tl) {
    super(tx, encoder, tl);
    this.tx = tx;
    this.encoder = encoder;
    this.tl = tl;
  }

  @Override
  public void set(ByteSequence row, Column col, ByteSequence value) {
    tx.set(row, col, value);
  }

  @Override
  public void delete(ByteSequence row, Column col) {
    tx.delete(row, col);
  }

  public RowAction<ValueSetter,VisAction,KeyBuilder> set() {
    return tl.new RowAction<ValueSetter,VisAction,KeyBuilder>(new KeyBuilder());
  }

  public RowAction<Deleted,VisDeleter,DeleteKeyBuilder> delete() {
    return tl.new RowAction<Deleted,VisDeleter,DeleteKeyBuilder>(new DeleteKeyBuilder());
  }
}
