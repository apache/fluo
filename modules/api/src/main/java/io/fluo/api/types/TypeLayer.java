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

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.Snapshot;
import io.fluo.api.client.Transaction;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import org.apache.accumulo.core.security.ColumnVisibility;

public class TypeLayer {

  private Encoder encoder;

  abstract static class RowColumnBuilder<TC,TQ> {
    abstract void setRow(Bytes r);

    abstract void setFamily(Bytes f);

    abstract TQ setQualifier(Bytes q);

    abstract TC setColumn(Column c);
  }

  private static class ColumnBuilder extends RowColumnBuilder<Column,VisibilityBuilder> {

    private Bytes family;

    @Override
    void setRow(Bytes r) {
      throw new UnsupportedOperationException();
    }

    @Override
    void setFamily(Bytes f) {
      this.family = f;
    }

    @Override
    VisibilityBuilder setQualifier(Bytes q) {
      return new VisibilityBuilder(new Column(family, q));
    }

    @Override
    Column setColumn(Column c) {
      return c;
    }

  }

  public static class VisibilityBuilder {
    private Column column;

    private VisibilityBuilder(Column column) {
      this.column = column;
    }

    public Column vis() {
      return column;
    }

    public Column vis(ColumnVisibility cv) {
      column.setVisibility(cv);
      return column;
    }
  }

  public class RowAction<TC,TQ,R extends RowColumnBuilder<TC,TQ>> {

    private R result;

    RowAction(R result) {
      this.result = result;
    }

    public FamilyAction<TC,TQ,R> row(String row) {
      result.setRow(encoder.encode(row));
      return new FamilyAction<TC,TQ,R>(result);
    }

    public FamilyAction<TC,TQ,R> row(int row) {
      result.setRow(encoder.encode(row));
      return new FamilyAction<TC,TQ,R>(result);
    }

    public FamilyAction<TC,TQ,R> row(long row) {
      result.setRow(encoder.encode(row));
      return new FamilyAction<TC,TQ,R>(result);
    }

    public FamilyAction<TC,TQ,R> row(byte[] row) {
      result.setRow(Bytes.wrap(row));
      return new FamilyAction<TC,TQ,R>(result);
    }

    public FamilyAction<TC,TQ,R> row(Bytes row) {
      result.setRow(row);
      return new FamilyAction<TC,TQ,R>(result);
    }
  }



  public class FamilyAction<TC,TQ,R extends RowColumnBuilder<TC,TQ>> {

    private R result;

    FamilyAction(R result) {
      this.result = result;
    }

    public QualifierAction<TC,TQ,R> fam(String family) {
      result.setFamily(encoder.encode(family));
      return new QualifierAction<TC,TQ,R>(result);
    }

    public QualifierAction<TC,TQ,R> fam(int family) {
      result.setFamily(encoder.encode(family));
      return new QualifierAction<TC,TQ,R>(result);
    }

    public QualifierAction<TC,TQ,R> fam(long family) {
      result.setFamily(encoder.encode(family));
      return new QualifierAction<TC,TQ,R>(result);
    }

    public QualifierAction<TC,TQ,R> fam(byte[] family) {
      result.setFamily(Bytes.wrap(family));
      return new QualifierAction<TC,TQ,R>(result);
    }

    public QualifierAction<TC,TQ,R> fam(Bytes family) {
      result.setFamily(family);
      return new QualifierAction<TC,TQ,R>(result);
    }

    public TC col(Column c) {
      return result.setColumn(c);
    }
  }

  public class QualifierAction<TC,TQ,R extends RowColumnBuilder<TC,TQ>> {

    private R result;

    QualifierAction(R result) {
      this.result = result;
    }

    public TQ qual(String qualifier) {
      return result.setQualifier(encoder.encode(qualifier));
    }

    public TQ qual(int qualifier) {
      return result.setQualifier(encoder.encode(qualifier));
    }

    public TQ qual(long qualifier) {
      return result.setQualifier(encoder.encode(qualifier));
    }

    public TQ qual(byte[] qualifier) {
      return result.setQualifier(Bytes.wrap(qualifier));
    }

    public TQ qual(Bytes qualifier) {
      return result.setQualifier(qualifier);
    }

  }

  public TypeLayer(Encoder encoder) {
    this.encoder = encoder;
  }

  public Column newColumn(String cf, String cq) {
    return newColumn().fam(cf).qual(cq).vis();
  }

  public FamilyAction<Column,VisibilityBuilder,ColumnBuilder> newColumn() {
    return new FamilyAction<Column,VisibilityBuilder,ColumnBuilder>(new ColumnBuilder());
  }

  public TypedSnapshot snapshot(FluoClient client) {
    return snapshot(client.newSnapshot());
  }

  public TypedSnapshot snapshot(Snapshot snap) {
    return new TypedSnapshot(snap, encoder, this);
  }

  public TypedTransaction transaction(Transaction tx) {
    return new TypedTransaction(tx, encoder, this);
  }
}
