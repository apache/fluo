package accismus.api.types;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

import accismus.api.Column;
import accismus.api.Transaction;

public class TypedTransaction extends TypedSnapshot implements Transaction {

  private Transaction tx;
  private Encoder encoder;

  public class BytesEncoder {
    private ByteSequence row;
    private Column col;
    private boolean set = false;

    private void checkNotSet() {
      if (set)
        throw new IllegalStateException("Already set value");
    }

    private BytesEncoder(ByteSequence row, Column col) {
      this.row = row;
      this.col = col;
    }

    public void from(String s) {
      checkNotSet();
      tx.set(row, col, encoder.encodeString(s));
      set = true;
    }

    public void from(int i) {
      checkNotSet();
      tx.set(row, col, encoder.encodeInteger(i));
      set = true;
    }

    public void from(long l) {
      checkNotSet();
      tx.set(row, col, encoder.encodeLong(l));
      set = true;
    }

    public void from(byte[] ba) {
      checkNotSet();
      tx.set(row, col, new ArrayByteSequence(ba));
      set = true;
    }
  }

  public TypedTransaction(Transaction tx, Encoder encoder) {
    super(tx, encoder);
    this.tx = tx;
    this.encoder = encoder;
  }

  @Override
  public void set(ByteSequence row, Column col, ByteSequence value) {
    tx.set(row, col, value);
  }

  @Override
  public void delete(ByteSequence row, Column col) {
    tx.delete(row, col);
  }

  public BytesEncoder sete(ByteSequence row, Column col) {
    return new BytesEncoder(row, col);
  }

  public BytesEncoder sete(String row, Column col) {
    return new BytesEncoder(encoder.encodeString(row), col);
  }

  public void delete(String row, Column col) {
    tx.delete(encoder.encodeString(row), col);
  }
}
