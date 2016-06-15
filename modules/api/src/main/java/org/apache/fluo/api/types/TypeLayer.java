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

import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

/**
 * A simple convenience layer for Fluo. This layer attempts to make the following common operations
 * easier.
 * 
 * <UL>
 * <LI>Working with different types.
 * <LI>Supplying default values
 * <LI>Dealing with null return types.
 * <LI>Working with row/column and column maps
 * </UL>
 * 
 * <p>
 * This layer was intentionally loosely coupled with the basic API. This allows other convenience
 * layers for Fluo to build directly on the basic API w/o having to consider the particulars of this
 * layer. Also its expected that integration with other languages may only use the basic API.
 * </p>
 * 
 * <h3>Using</h3>
 * 
 * <p>
 * A TypeLayer is created with a certain encoder that is used for converting from bytes to
 * primitives and visa versa. In order to ensure that all of your code uses the same encoder, its
 * probably best to centralize the choice of an encoder within your project. There are many ways do
 * to this, below is an example of one way to centralize and use.
 * </p>
 * 
 * <pre>
 * <code>
 * 
 *   public class MyTypeLayer extends TypeLayer {
 *     public MyTypeLayer() {
 *       super(new MyEncoder());
 *     }
 *   }
 *   
 *   public class MyObserver extends TypedObserver {
 *     MyObserver(){
 *       super(new MyTypeLayer());
 *     }
 *     
 *     public abstract void process(TypedTransaction tx, Bytes row, Column col){
 *       //do something w/ typed transaction
 *     }
 *   }
 *   
 *   public class MyUtil {
 *      //A little util to print out some stuff
 *      public void printStuff(Snapshot snap, byte[] row){
 *        TypedSnapshot tsnap = new MytTypeLayer().wrap(snap);
 *        
 *        System.out.println(tsnap.get().row(row).fam("b90000").qual(137).toString("NP"));
 *      } 
 *   }
 * </code>
 * </pre>
 * 
 * <h3>Working with different types</h3>
 * 
 * <p>
 * The following example code shows using the basic fluo API with different types.
 * </p>
 * 
 * <pre>
 * <code>
 * 
 *   void process(Transaction tx, byte[] row, byte[] cf, int cq, long val){
 *     tx.set(Bytes.of(row), new Column(Bytes.of(cf), Bytes.of(Integer.toString(cq))),
 *        Bytes.of(Long.toString(val));
 *   }
 * </code>
 * </pre>
 * 
 * <p>
 * Alternatively, the same thing can be written using a {@link TypedTransactionBase} in the
 * following way. Because row(), fam(), qual(), and set() each take many different types, this
 * enables many different permutations that would not be achievable with overloading.
 * </p>
 * 
 * <pre>
 * <code>
 * 
 *   void process(TypedTransaction tx, byte[] r, byte[] cf, int cq, long v){
 *     tx.mutate().row(r).fam(cf).qual(cq).set(v);
 *   }
 * </code>
 * </pre>
 * 
 * <h3>Default values</h3>
 * 
 * <p>
 * The following example code shows using the basic fluo API to read a value and default to zero if
 * it does not exist.
 * </p>
 * 
 * <pre>
 * <code>
 * 
 *   void add(Transaction tx, byte[] row, Column col, long amount){
 *     
 *     long balance = 0;
 *     Bytes bval = tx.get(Bytes.of(row), col);
 *     if(bval != null)
 *       balance = Long.parseLong(bval.toString());
 *     
 *     balance += amount;
 *     
 *     tx.set(Bytes.of(row), col, Bytes.of(Long.toString(amount)));
 *     
 *   }
 * </code>
 * </pre>
 * 
 * <p>
 * Alternatively, the same thing can be written using a {@link TypedTransactionBase} in the
 * following way. This code avoids the null check by supplying a default value of zero.
 * </p>
 * 
 * <pre>
 * <code>
 * 
 *   void add(TypedTransaction tx, byte[] r, Column c, long amount){
 *     long balance = tx.get().row(r).col(c).toLong(0);
 *     balance += amount;
 *     tx.mutate().row(r).col(c).set(balance);
 *   }
 * </code>
 * </pre>
 * 
 * <p>
 * For this particular case, shorter code can be written by using the increment method.
 * </p>
 * 
 * <pre>
 * <code>
 * 
 *   void add(TypedTransaction tx, byte[] r, Column c, long amount){
 *     tx.mutate().row(r).col(c).increment(amount);
 *   }
 * </code>
 * </pre>
 * 
 * <h3>Null return types</h3>
 * 
 * <p>
 * When using the basic API, you must ensure the return type is not null before converting a string
 * or long.
 * </p>
 * 
 * <pre>
 * <code>
 * 
 *   void process(Transaction tx, byte[] row, Column col, long amount) {
 *     Bytes val =  tx.get(Bytes.of(row), col);
 *     if(val == null)
 *       return;   
 *     long balance = Long.parseLong(val.toString());
 *   }
 * </code>
 * </pre>
 * 
 * <p>
 * With {@link TypedTransactionBase} if no default value is supplied, then the null is passed
 * through.
 * </p>
 * 
 * <pre>
 * <code>
 * 
 *   void process(TypedTransaction tx, byte[] r, Column c, long amount){
 *     Long balance =  tx.get().row(r).col(c).toLong();
 *     if(balance == null)
 *       return;   
 *   }
 * </code>
 * </pre>
 * 
 * <h3>Defaulted maps</h3>
 * 
 * <p>
 * The operations that return maps, return defaulted maps which make it easy to specify defaults and
 * avoid null.
 * </p>
 * 
 * <pre>
 * {@code
 *   // pretend this method has curly braces.  javadoc has issues with less than.
 * 
 *   void process(TypedTransaction tx, byte[] r, Column c1, Column c2, Column c3, long amount)
 * 
 *     Map<Column, Value> columns = tx.get().row(r).columns(c1,c2,c3);
 *     
 *     // If c1 does not exist in map, a Value that wraps null will be returned.
 *     // When c1 does not exist val1 will be set to null and no NPE will be thrown.
 *     String val1 = columns.get(c1).toString();
 *     
 *     // If c2 does not exist in map, then val2 will be set to empty string.
 *     String val2 = columns.get(c2).toString("");
 *     
 *     // If c3 does not exist in map, then val9 will be set to 9.
 *     Long val3 = columns.get(c3).toLong(9);
 * }
 * </pre>
 * 
 * <p>
 * This also applies to getting sets of rows.
 * </p>
 * 
 * <pre>
 * {@code
 *   // pretend this method has curly braces.  javadoc has issues with less than.
 * 
 *   void process(TypedTransaction tx, List<String> rows, Column c1, Column c2, Column c3,
 *     long amount)
 * 
 *     Map<String,Map<Column,Value>> rowCols =
 *        tx.get().rowsString(rows).columns(c1,c2,c3).toStringMap();
 *     
 *     // this will set val1 to null if row does not exist in map and/or column does not
 *     // exist in child map
 *     String val1 = rowCols.get("row1").get(c1).toString();
 * }
 * </pre>
 */

public class TypeLayer {

  private Encoder encoder;

  static class Data {
    Bytes row;
    Bytes family;
    Bytes qual;
    Bytes vis;

    Column getCol() {
      if (qual == null) {
        return new Column(family);
      } else if (vis == null) {
        return new Column(family, qual);
      } else {
        return new Column(family, qual, vis);
      }
    }
  }

  public abstract class RowMethods<R> {

    abstract R create(Data data);

    public R row(String row) {
      return row(encoder.encode(row));
    }

    public R row(int row) {
      return row(encoder.encode(row));
    }

    public R row(long row) {
      return row(encoder.encode(row));
    }

    public R row(byte[] row) {
      return row(Bytes.of(row));
    }

    public R row(ByteBuffer row) {
      return row(Bytes.of(row));
    }

    public R row(Bytes row) {
      Data data = new Data();
      data.row = row;
      R result = create(data);
      return result;
    }
  }

  public abstract class SimpleFamilyMethods<R1> {

    protected Data data;

    SimpleFamilyMethods(Data data) {
      this.data = data;
    }

    abstract R1 create1(Data data);

    public R1 fam(String family) {
      return fam(encoder.encode(family));
    }

    public R1 fam(int family) {
      return fam(encoder.encode(family));
    }

    public R1 fam(long family) {
      return fam(encoder.encode(family));
    }

    public R1 fam(byte[] family) {
      return fam(Bytes.of(family));
    }

    public R1 fam(ByteBuffer family) {
      return fam(Bytes.of(family));
    }

    public R1 fam(Bytes family) {
      data.family = family;
      return create1(data);
    }
  }

  public abstract class FamilyMethods<R1, R2> extends SimpleFamilyMethods<R1> {

    FamilyMethods(Data data) {
      super(data);
    }

    abstract R2 create2(Data data);

    public R2 col(Column col) {
      data.family = col.getFamily();
      data.qual = col.getQualifier();
      data.vis = col.getVisibility();
      return create2(data);
    }
  }

  public abstract class QualifierMethods<R> {

    protected Data data;

    QualifierMethods(Data data) {
      this.data = data;
    }

    abstract R create(Data data);

    public R qual(String qualifier) {
      return qual(encoder.encode(qualifier));
    }

    public R qual(int qualifier) {
      return qual(encoder.encode(qualifier));
    }

    public R qual(long qualifier) {
      return qual(encoder.encode(qualifier));
    }

    public R qual(byte[] qualifier) {
      return qual(Bytes.of(qualifier));
    }

    public R qual(ByteBuffer qualifier) {
      return qual(Bytes.of(qualifier));
    }

    public R qual(Bytes qualifier) {
      data.qual = qualifier;
      return create(data);
    }
  }

  public static class VisibilityMethods {

    private Data data;

    public VisibilityMethods(Data data) {
      this.data = data;
    }

    public Column vis() {
      return new Column(data.family, data.qual);
    }

    public Column vis(String cv) {
      return vis(Bytes.of(cv));
    }

    public Column vis(Bytes cv) {
      return new Column(data.family, data.qual, cv);
    }

    public Column vis(ByteBuffer cv) {
      return vis(Bytes.of(cv));
    }

    public Column vis(byte[] cv) {
      return vis(Bytes.of(cv));
    }
  }

  public class CQB extends QualifierMethods<VisibilityMethods> {
    CQB(Data data) {
      super(data);
    }

    @Override
    VisibilityMethods create(Data data) {
      return new VisibilityMethods(data);
    }
  }

  public class CFB extends SimpleFamilyMethods<CQB> {
    CFB() {
      super(new Data());
    }

    @Override
    CQB create1(Data data) {
      return new CQB(data);
    }
  }

  public TypeLayer(Encoder encoder) {
    this.encoder = encoder;
  }

  /**
   * Initiates the chain of calls needed to build a column.
   * 
   * @return a column builder
   */
  public CFB bc() {
    return new CFB();
  }

  public TypedSnapshot wrap(Snapshot snap) {
    return new TypedSnapshot(snap, encoder, this);
  }

  public TypedTransactionBase wrap(TransactionBase tx) {
    return new TypedTransactionBase(tx, encoder, this);
  }

  public TypedTransaction wrap(Transaction tx) {
    return new TypedTransaction(tx, encoder, this);
  }
}
