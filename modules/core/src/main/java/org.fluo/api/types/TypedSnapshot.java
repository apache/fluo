package org.fluo.api.types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections.map.DefaultedMap;

import org.fluo.api.Column;
import org.fluo.api.RowIterator;
import org.fluo.api.ScannerConfiguration;
import org.fluo.api.Snapshot;
import org.fluo.api.types.TypeLayer.RowAction;
import org.fluo.api.types.TypeLayer.RowColumnBuilder;

//TODO need to refactor column to use Encoder

public class TypedSnapshot implements Snapshot {

  private Snapshot snapshot;
  private Encoder encoder;
  private TypeLayer tl;

  private class KeyBuilder extends RowColumnBuilder<Value,VisBytesDecoder> {

    private ByteSequence family;
    private ByteSequence row;

    @Override
    void setRow(ByteSequence r) {
      this.row = r;
    }

    @Override
    void setFamily(ByteSequence f) {
      this.family = f;
    }

    @Override
    public VisBytesDecoder setQualifier(ByteSequence q) {
      return new VisBytesDecoder(row, new Column(family, q));
    }

    @Override
    public Value setColumn(Column c) {
      try {
        return new Value(snapshot.get(row, c));
      } catch (Exception e) {
        // TODO
        if (e instanceof RuntimeException)
          throw (RuntimeException) e;
        throw new RuntimeException(e);
      }
    }

  }

  public class VisBytesDecoder extends Value {

    private ByteSequence row;
    private Column col;
    private boolean gotBytes = false;

    ByteSequence getBytes() {
      if (!gotBytes) {
        try {
          super.bytes = snapshot.get(row, col);
          gotBytes = true;
        } catch (Exception e) {
          if (e instanceof RuntimeException)
            throw (RuntimeException) e;
          throw new RuntimeException(e);
        }
      }

      return super.getBytes();
    }

    VisBytesDecoder(ByteSequence row, Column col) {
      super(null);
      this.row = row;
      this.col = col;
    }

    public Value vis(ColumnVisibility cv) {
      col.setVisibility(cv);
      gotBytes = false;
      return new Value(getBytes());
    }
  }

  public class Value {
    ByteSequence bytes;

    ByteSequence getBytes() {
      return bytes;
    }

    private Value(ByteSequence bytes) {
      this.bytes = bytes;
    }

    public Integer toInteger() {
      if (getBytes() == null)
        return null;
      return encoder.decodeInteger(getBytes());
    }

    public int toInteger(int defaultValue) {
      if (getBytes() == null)
        return defaultValue;
      return encoder.decodeInteger(getBytes());
    }

    public Long toLong() {
      if (getBytes() == null)
        return null;
      return encoder.decodeLong(getBytes());
    }

    public long toLong(int defaultValue) {
      if (getBytes() == null)
        return defaultValue;
      return encoder.decodeLong(getBytes());
    }

    @Override
    public String toString() {
      if (getBytes() == null)
        return null;
      return encoder.decodeString(getBytes());
    }

    public String toString(String defaultValue) {
      if (getBytes() == null)
        return defaultValue;
      return encoder.decodeString(getBytes());
    }

    public byte[] toBytes() {
      if (getBytes() == null)
        return null;
      return getBytes().toArray();
    }

    public byte[] toBytes(byte[] defaultValue) {
      if (getBytes() == null)
        return defaultValue;
      return getBytes().toArray();
    }
  }

  TypedSnapshot(Snapshot snapshot, Encoder encoder, TypeLayer tl) {
    this.snapshot = snapshot;
    this.encoder = encoder;
    this.tl = tl;
  }

  @Override
  public ByteSequence get(ByteSequence row, Column column) throws Exception {
    return snapshot.get(row, column);
  }

  @Override
  public Map<Column,ByteSequence> get(ByteSequence row, Set<Column> columns) throws Exception {
    return snapshot.get(row, columns);
  }

  @Override
  public RowIterator get(ScannerConfiguration config) throws Exception {
    return snapshot.get(config);
  }

  @Override
  public Map<ByteSequence,Map<Column,ByteSequence>> get(Collection<ByteSequence> rows, Set<Column> columns) throws Exception {
    return snapshot.get(rows, columns);
  }

  public RowAction<Value,VisBytesDecoder,KeyBuilder> get() {
    return tl.new RowAction<Value,VisBytesDecoder,KeyBuilder>(new KeyBuilder());
  }


  @SuppressWarnings("unchecked")
  public Map<Column,Value> getd(ByteSequence row, Set<Column> columns) throws Exception {
    Map<Column,ByteSequence> map = snapshot.get(row, columns);
    Map<Column,Value> ret = new HashMap<Column,Value>();

    Set<Entry<Column,ByteSequence>> es = map.entrySet();
    for (Entry<Column,ByteSequence> entry : es) {
      ret.put(entry.getKey(), new Value(entry.getValue()));
    }

    return DefaultedMap.decorate(ret, new Value(null));
  }

  public Map<Column,Value> getd(String row, Set<Column> columns) throws Exception {
    return getd(encoder.encode(row), columns);
  }

  @SuppressWarnings("unchecked")
  public Map<String,Map<Column,Value>> getd(Collection<String> rows, Set<Column> columns) throws Exception {
    ArrayList<ByteSequence> bsRows = new ArrayList<ByteSequence>(rows.size());
    for (String row : rows) {
      bsRows.add(encoder.encode(row));
    }

    Map<ByteSequence,Map<Column,ByteSequence>> in = snapshot.get(bsRows, columns);
    Map<String,Map<Column,Value>> out = new HashMap<String,Map<Column,Value>>();

    for (Entry<ByteSequence,Map<Column,ByteSequence>> rowEntry : in.entrySet()) {
      Map<Column,Value> outCols = new HashMap<Column,Value>();
      for (Entry<Column,ByteSequence> colEntry : rowEntry.getValue().entrySet()) {
        outCols.put(colEntry.getKey(), new Value(colEntry.getValue()));
      }
      out.put(encoder.decodeString(rowEntry.getKey()), DefaultedMap.decorate(outCols, new Value(null)));
    }

    return DefaultedMap.decorate(out, new DefaultedMap(new Value(null)));
  }
}
