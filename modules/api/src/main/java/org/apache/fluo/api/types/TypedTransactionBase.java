/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.api.types;

import java.nio.ByteBuffer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.AlreadySetException;
import org.apache.fluo.api.types.TypeLayer.Data;
import org.apache.fluo.api.types.TypeLayer.FamilyMethods;
import org.apache.fluo.api.types.TypeLayer.QualifierMethods;
import org.apache.fluo.api.types.TypeLayer.RowMethods;

/**
 * A {@link TransactionBase} that uses a {@link TypeLayer}
 *
 * @since 1.0.0
 */
public class TypedTransactionBase extends TypedSnapshotBase implements TransactionBase {

  private final TransactionBase tx;
  private final Encoder encoder;
  private final TypeLayer tl;

  /**
   * @since 1.0.0
   */
  public class Mutator {

    private boolean set = false;
    protected Data data;

    public Mutator(Data data) {
      this.data = data;
    }

    void checkNotSet() {
      if (set) {
        throw new IllegalStateException("Already set value");
      }
    }

    public void set(Bytes bytes) throws AlreadySetException {
      checkNotSet();
      tx.set(data.row, data.getCol(), bytes);
      set = true;
    }

    public void set(String s) throws AlreadySetException {
      set(encoder.encode(s));
    }

    public void set(int i) throws AlreadySetException {
      set(encoder.encode(i));
    }

    public void set(long l) throws AlreadySetException {
      set(encoder.encode(l));
    }

    public void set(float f) throws AlreadySetException {
      set(encoder.encode(f));
    }

    public void set(double d) throws AlreadySetException {
      set(encoder.encode(d));
    }

    public void set(boolean b) throws AlreadySetException {
      set(encoder.encode(b));
    }

    public void set(byte[] ba) throws AlreadySetException {
      set(Bytes.of(ba));
    }

    public void set(ByteBuffer bb) throws AlreadySetException {
      set(Bytes.of(bb));
    }

    /**
     * Set an empty value
     */
    public void set() throws AlreadySetException {
      set(Bytes.EMPTY);
    }

    /**
     * Reads the current value of the row/column, adds i, sets the sum. If the row/column does not
     * have a current value, then it defaults to zero.
     *
     * @param i Integer increment amount
     * @throws AlreadySetException if value was previously set in transaction
     */
    public void increment(int i) throws AlreadySetException {
      checkNotSet();
      Bytes val = tx.get(data.row, data.getCol());
      int v = 0;
      if (val != null) {
        v = encoder.decodeInteger(val);
      }
      tx.set(data.row, data.getCol(), encoder.encode(v + i));
    }

    /**
     * Reads the current value of the row/column, adds l, sets the sum. If the row/column does not
     * have a current value, then it defaults to zero.
     *
     * @param l Long increment amount
     * @throws AlreadySetException if value was previously set in transaction
     */
    public void increment(long l) throws AlreadySetException {
      checkNotSet();
      Bytes val = tx.get(data.row, data.getCol());
      long v = 0;
      if (val != null) {
        v = encoder.decodeLong(val);
      }
      tx.set(data.row, data.getCol(), encoder.encode(v + l));
    }

    public void delete() throws AlreadySetException {
      checkNotSet();
      tx.delete(data.row, data.getCol());
      set = true;
    }

    public void weaklyNotify() {
      checkNotSet();
      tx.setWeakNotification(data.row, data.getCol());
      set = true;
    }

  }

  /**
   * @since 1.0.0
   */
  public class VisibilityMutator extends Mutator {

    public VisibilityMutator(Data data) {
      super(data);
    }

    public Mutator vis(String cv) {
      checkNotSet();
      data.vis = Bytes.of(cv);
      return new Mutator(data);
    }

    public Mutator vis(Bytes cv) {
      checkNotSet();
      data.vis = cv;
      return new Mutator(data);
    }

    public Mutator vis(byte[] cv) {
      checkNotSet();
      data.vis = Bytes.of(cv);
      return new Mutator(data);
    }

    public Mutator vis(ByteBuffer cv) {
      checkNotSet();
      data.vis = Bytes.of(cv);
      return new Mutator(data);
    }
  }

  /**
   * @since 1.0.0
   */
  public class MutatorQualifierMethods extends QualifierMethods<VisibilityMutator> {

    MutatorQualifierMethods(Data data) {
      tl.super(data);
    }

    @Override
    VisibilityMutator create(Data data) {
      return new VisibilityMutator(data);
    }
  }

  /**
   * @since 1.0.0
   */
  public class MutatorFamilyMethods extends FamilyMethods<MutatorQualifierMethods, Mutator> {

    MutatorFamilyMethods(Data data) {
      tl.super(data);
    }

    @Override
    MutatorQualifierMethods create1(Data data) {
      return new MutatorQualifierMethods(data);
    }

    @Override
    Mutator create2(Data data) {
      return new Mutator(data);
    }
  }

  /**
   * @since 1.0.0
   */
  public class MutatorRowMethods extends RowMethods<MutatorFamilyMethods> {

    MutatorRowMethods() {
      tl.super();
    }

    @Override
    MutatorFamilyMethods create(Data data) {
      return new MutatorFamilyMethods(data);
    }

  }

  @VisibleForTesting
  protected TypedTransactionBase(TransactionBase tx, Encoder encoder, TypeLayer tl) {
    super(tx, encoder, tl);
    this.tx = tx;
    this.encoder = encoder;
    this.tl = tl;
  }

  public MutatorRowMethods mutate() {
    return new MutatorRowMethods();
  }

  @Override
  public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
    tx.set(row, col, value);
  }

  @Override
  public void set(String row, Column col, String value) throws AlreadySetException {
    tx.set(row, col, value);
  }

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    tx.setWeakNotification(row, col);
  }

  @Override
  public void setWeakNotification(String row, Column col) {
    tx.setWeakNotification(row, col);
  }

  @Override
  public void delete(Bytes row, Column col) throws AlreadySetException {
    tx.delete(row, col);
  }

  @Override
  public void delete(String row, Column col) {
    tx.delete(row, col);
  }
}
