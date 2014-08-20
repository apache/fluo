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
package io.fluo.api.types;

import io.fluo.api.client.Transaction;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.TypeLayer.RowAction;
import io.fluo.api.types.TypeLayer.RowColumnBuilder;
import org.apache.accumulo.core.security.ColumnVisibility;

public class TypedTransaction extends TypedSnapshot implements Transaction {

  private Transaction tx;
  private Encoder encoder;
  private TypeLayer tl;

  private class MutateKeyBuilder extends RowColumnBuilder<Mutator,VisibilityMutator> {

    private Bytes row;
    private Bytes cf;

    @Override
    void setRow(Bytes r) {
      this.row = r;
    }

    @Override
    void setFamily(Bytes f) {
      this.cf = f;
    }

    @Override
    VisibilityMutator setQualifier(Bytes q) {
      return new VisibilityMutator(row, new Column(cf, q));
    }

    @Override
    Mutator setColumn(Column c) {
      return new Mutator(row, c);
    }

  }

  public class Mutator {

    private Bytes row;
    private Column col;
    private boolean set = false;

    void checkNotSet() {
      if (set)
        throw new IllegalStateException("Already set value");
    }

    Mutator(Bytes row, Column column) {
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
      Bytes val = tx.get(row, col);
      int v = 0;
      if (val != null)
        v = encoder.decodeInteger(val);
      tx.set(row, col, encoder.encode(v + i));
    }

    public void increment(long l) throws Exception {
      checkNotSet();
      Bytes val = tx.get(row, col);
      long v = 0;
      if (val != null)
        v = encoder.decodeLong(val);
      tx.set(row, col, encoder.encode(v + l));
    }

    public void set(byte[] ba) {
      checkNotSet();
      tx.set(row, col, Bytes.wrap(ba));
      set = true;
    }

    /**
     * Set an empty value
     */
    public void set() {
      checkNotSet();
      tx.set(row, col, Bytes.wrap(new byte[0]));
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

    VisibilityMutator(Bytes row, Column column) {
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
  public void set(Bytes row, Column col, Bytes value) {
    tx.set(row, col, value);
  }

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    tx.setWeakNotification(row, col);
  }

  @Override
  public void delete(Bytes row, Column col) {
    tx.delete(row, col);
  }
}
