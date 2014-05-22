package accismus.api.types;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.ByteSequence;

import accismus.api.Column;
import accismus.api.RowIterator;
import accismus.api.ScannerConfiguration;
import accismus.api.Snapshot;

//TODO need to refactor column to use Encoder

public class TypedSnapshot implements Snapshot {

  private Snapshot snapshot;
  private Encoder encoder;

  public class BytesDecoder {
    private ByteSequence bytes;

    private BytesDecoder(ByteSequence bytes) {
      this.bytes = bytes;
    }

    public Integer toInteger() {
      if (bytes == null)
        return null;
      return encoder.decodeInteger(bytes);
    }

    public int toInteger(int defaultValue) {
      if (bytes == null)
        return defaultValue;
      return encoder.decodeInteger(bytes);
    }

    public Long toLong() {
      if (bytes == null)
        return null;
      return encoder.decodeLong(bytes);
    }

    public long toLong(int defaultValue) {
      if (bytes == null)
        return defaultValue;
      return encoder.decodeLong(bytes);
    }

    @Override
    public String toString() {
      if (bytes == null)
        return null;
      return encoder.decodeString(bytes);
    }

    public String toString(String defaultValue) {
      if (bytes == null)
        return defaultValue;
      return encoder.decodeString(bytes);
    }

    public byte[] toBytes() {
      if (bytes == null)
        return null;
      return bytes.toArray();
    }

    public byte[] toBytes(byte[] defaultValue) {
      if (bytes == null)
        return defaultValue;
      return bytes.toArray();
    }
  }

  public TypedSnapshot(Snapshot snapshot, Encoder encoder) {
    this.snapshot = snapshot;
    this.encoder = encoder;
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

  public BytesDecoder getd(ByteSequence row, Column column) throws Exception {
    return new BytesDecoder(snapshot.get(row, column));
  }

  public BytesDecoder getd(String row, Column column) throws Exception {
    return new BytesDecoder(snapshot.get(encoder.encodeString(row), column));
  }

  public Map<Column,BytesDecoder> getd(ByteSequence row, Set<Column> columns) throws Exception {
    Map<Column,ByteSequence> map = snapshot.get(row, columns);
    Map<Column,BytesDecoder> ret = new HashMap<Column,BytesDecoder>();

    Set<Entry<Column,ByteSequence>> es = map.entrySet();
    for (Entry<Column,ByteSequence> entry : es) {
      ret.put(entry.getKey(), new BytesDecoder(entry.getValue()));
    }

    return ret;
  }

  public Map<Column,BytesDecoder> getd(String row, Set<Column> columns) throws Exception {
    return getd(encoder.encodeString(row), columns);
  }
}
