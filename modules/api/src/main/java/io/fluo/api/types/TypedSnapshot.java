package io.fluo.api.types;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.fluo.api.client.Snapshot;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.types.TypeLayer.RowAction;
import io.fluo.api.types.TypeLayer.RowColumnBuilder;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections.map.DefaultedMap;

//TODO need to refactor column to use Encoder

public class TypedSnapshot implements Snapshot {

  private Snapshot snapshot;
  private Encoder encoder;
  private TypeLayer tl;

  private class KeyBuilder extends RowColumnBuilder<Value,VisBytesDecoder> {

    private Bytes family;
    private Bytes row;

    @Override
    void setRow(Bytes r) {
      this.row = r;
    }

    @Override
    void setFamily(Bytes f) {
      this.family = f;
    }

    @Override
    public VisBytesDecoder setQualifier(Bytes q) {
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

    private Bytes row;
    private Column col;
    private boolean gotBytes = false;

    Bytes getBytes() {
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

    VisBytesDecoder(Bytes row, Column col) {
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
    Bytes bytes;

    Bytes getBytes() {
      return bytes;
    }

    private Value(Bytes bytes) {
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
  public Bytes get(Bytes row, Column column) throws Exception {
    return snapshot.get(row, column);
  }

  @Override
  public Map<Column,Bytes> get(Bytes row, Set<Column> columns) throws Exception {
    return snapshot.get(row, columns);
  }

  @Override
  public RowIterator get(ScannerConfiguration config) throws Exception {
    return snapshot.get(config);
  }

  @Override
  public Map<Bytes,Map<Column,Bytes>> get(Collection<Bytes> rows, Set<Column> columns) throws Exception {
    return snapshot.get(rows, columns);
  }

  public RowAction<Value,VisBytesDecoder,KeyBuilder> get() {
    return tl.new RowAction<Value,VisBytesDecoder,KeyBuilder>(new KeyBuilder());
  }


  @SuppressWarnings("unchecked")
  public Map<Column,Value> getd(Bytes row, Set<Column> columns) throws Exception {
    Map<Column,Bytes> map = snapshot.get(row, columns);
    Map<Column,Value> ret = new HashMap<Column,Value>();

    Set<Entry<Column,Bytes>> es = map.entrySet();
    for (Entry<Column,Bytes> entry : es) {
      ret.put(entry.getKey(), new Value(entry.getValue()));
    }

    return DefaultedMap.decorate(ret, new Value(null));
  }

  public Map<Column,Value> getd(String row, Set<Column> columns) throws Exception {
    return getd(encoder.encode(row), columns);
  }

  @SuppressWarnings("unchecked")
  public Map<String,Map<Column,Value>> getd(Collection<String> rows, Set<Column> columns) throws Exception {
    ArrayList<Bytes> bsRows = new ArrayList<Bytes>(rows.size());
    for (String row : rows) {
      bsRows.add(encoder.encode(row));
    }

    Map<Bytes,Map<Column,Bytes>> in = snapshot.get(bsRows, columns);
    Map<String,Map<Column,Value>> out = new HashMap<String,Map<Column,Value>>();

    for (Entry<Bytes,Map<Column,Bytes>> rowEntry : in.entrySet()) {
      Map<Column,Value> outCols = new HashMap<Column,Value>();
      for (Entry<Column,Bytes> colEntry : rowEntry.getValue().entrySet()) {
        outCols.put(colEntry.getKey(), new Value(colEntry.getValue()));
      }
      out.put(encoder.decodeString(rowEntry.getKey()), DefaultedMap.decorate(outCols, new Value(null)));
    }

    return DefaultedMap.decorate(out, new DefaultedMap(new Value(null)));
  }
}
