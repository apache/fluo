package org.fluo.api.types;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;

import org.fluo.api.Column;
import org.fluo.api.Snapshot;
import org.fluo.api.SnapshotFactory;
import org.fluo.api.Transaction;

public class TypeLayer {

  private Encoder encoder;

  abstract static class RowColumnBuilder<TC,TQ> {
    abstract void setRow(ByteSequence r);

    abstract void setFamily(ByteSequence f);

    abstract TQ setQualifier(ByteSequence q);

    abstract TC setColumn(Column c);
  }

  private static class ColumnBuilder extends RowColumnBuilder<Column,VisibilityBuilder> {

    private ByteSequence family;

    @Override
    void setRow(ByteSequence r) {
      throw new UnsupportedOperationException();
    }

    @Override
    void setFamily(ByteSequence f) {
      this.family = f;
    }

    @Override
    VisibilityBuilder setQualifier(ByteSequence q) {
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
      result.setRow(new ArrayByteSequence(row));
      return new FamilyAction<TC,TQ,R>(result);
    }

    public FamilyAction<TC,TQ,R> row(ByteSequence row) {
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
      result.setFamily(new ArrayByteSequence(family));
      return new QualifierAction<TC,TQ,R>(result);
    }

    public QualifierAction<TC,TQ,R> fam(ByteSequence family) {
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
      return result.setQualifier(new ArrayByteSequence(qualifier));
    }

    public TQ qual(ByteSequence qualifier) {
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

  public TypedSnapshot snapshot(SnapshotFactory sf) {
    return new TypedSnapshot(sf.createSnapshot(), encoder, this);
  }

  public TypedSnapshot snapshot(Snapshot snap) {
    return new TypedSnapshot(snap, encoder, this);
  }

  public TypedTransaction transaction(Transaction tx) {
    return new TypedTransaction(tx, encoder, this);
  }
}
