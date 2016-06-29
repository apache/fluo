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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.commons.collections.map.DefaultedMap;
import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.config.ScannerConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.iterator.RowIterator;
import org.apache.fluo.api.types.TypeLayer.Data;
import org.apache.fluo.api.types.TypeLayer.FamilyMethods;
import org.apache.fluo.api.types.TypeLayer.QualifierMethods;
import org.apache.fluo.api.types.TypeLayer.RowMethods;

// TODO need to refactor column to use Encoder

/**
 * A {@link SnapshotBase} that uses a {@link TypeLayer}
 *
 * @since 1.0.0
 */
public class TypedSnapshotBase implements SnapshotBase {

  private SnapshotBase snapshot;
  private Encoder encoder;
  private TypeLayer tl;

  /**
   * @since 1.0.0
   */
  public class VisibilityMethods extends Value {

    public VisibilityMethods(Data data) {
      super(data);
    }

    public Value vis(Bytes cv) {
      data.vis = cv;
      return new Value(data);
    }

    public Value vis(byte[] cv) {
      data.vis = Bytes.of(cv);
      return new Value(data);
    }

    public Value vis(ByteBuffer bb) {
      data.vis = Bytes.of(bb);
      return new Value(data);
    }

    public Value vis(String cv) {
      data.vis = Bytes.of(cv);
      return new Value(data);
    }
  }

  /**
   * @since 1.0.0
   */
  public class Value {
    private Bytes bytes;
    private boolean gotBytes = false;
    protected Data data;

    public Bytes getBytes() {
      if (!gotBytes) {
        try {
          bytes = snapshot.get(data.row, data.getCol());
          gotBytes = true;
        } catch (Exception e) {
          if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          }
          throw new RuntimeException(e);
        }
      }

      return bytes;
    }

    private Value(Bytes bytes) {
      this.bytes = bytes;
      this.gotBytes = true;
    }

    private Value(Data data) {
      this.data = data;
      this.gotBytes = false;
    }

    public Integer toInteger() {
      if (getBytes() == null) {
        return null;
      }
      return encoder.decodeInteger(getBytes());
    }

    public int toInteger(int defaultValue) {
      if (getBytes() == null) {
        return defaultValue;
      }
      return encoder.decodeInteger(getBytes());
    }

    public Long toLong() {
      if (getBytes() == null) {
        return null;
      }
      return encoder.decodeLong(getBytes());
    }

    public long toLong(long defaultValue) {
      if (getBytes() == null) {
        return defaultValue;
      }
      return encoder.decodeLong(getBytes());
    }

    @Override
    public String toString() {
      if (getBytes() == null) {
        return null;
      }
      return encoder.decodeString(getBytes());
    }

    public String toString(String defaultValue) {
      if (getBytes() == null) {
        return defaultValue;
      }
      return encoder.decodeString(getBytes());
    }

    public Float toFloat() {
      if (getBytes() == null) {
        return null;
      }
      return encoder.decodeFloat(getBytes());
    }

    public float toFloat(float defaultValue) {
      if (getBytes() == null) {
        return defaultValue;
      }
      return encoder.decodeFloat(getBytes());
    }

    public Double toDouble() {
      if (getBytes() == null) {
        return null;
      }
      return encoder.decodeDouble(getBytes());
    }

    public double toDouble(double defaultValue) {
      if (getBytes() == null) {
        return defaultValue;
      }
      return encoder.decodeDouble(getBytes());
    }

    public Boolean toBoolean() {
      if (getBytes() == null) {
        return null;
      }
      return encoder.decodeBoolean(getBytes());
    }

    public boolean toBoolean(boolean defaultValue) {
      if (getBytes() == null) {
        return defaultValue;
      }
      return encoder.decodeBoolean(getBytes());
    }

    public byte[] toBytes() {
      if (getBytes() == null) {
        return null;
      }
      return getBytes().toArray();
    }

    public byte[] toBytes(byte[] defaultValue) {
      if (getBytes() == null) {
        return defaultValue;
      }
      return getBytes().toArray();
    }

    public ByteBuffer toByteBuffer() {
      if (getBytes() == null) {
        return null;
      }
      return ByteBuffer.wrap(getBytes().toArray());
    }

    public ByteBuffer toByteBuffer(ByteBuffer defaultValue) {
      if (getBytes() == null) {
        return defaultValue;
      }
      return toByteBuffer();
    }

    @Override
    public int hashCode() {
      if (getBytes() == null) {
        return 0;
      }

      return getBytes().hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof Value) {
        Value ov = (Value) o;
        if (getBytes() == null) {
          return ov.getBytes() == null;
        } else {
          return getBytes().equals(ov.getBytes());
        }
      }

      return false;
    }
  }

  /**
   * @since 1.0.0
   */
  public class ValueQualifierBuilder extends QualifierMethods<VisibilityMethods> {

    ValueQualifierBuilder(Data data) {
      tl.super(data);
    }

    @Override
    VisibilityMethods create(Data data) {
      return new VisibilityMethods(data);
    }
  }

  /**
   * @since 1.0.0
   */
  public class ValueFamilyMethods extends FamilyMethods<ValueQualifierBuilder, Value> {

    ValueFamilyMethods(Data data) {
      tl.super(data);
    }

    @Override
    ValueQualifierBuilder create1(Data data) {
      return new ValueQualifierBuilder(data);
    }

    @Override
    Value create2(Data data) {
      return new Value(data);
    }

    public Map<Column, Value> columns(Set<Column> columns) {
      try {
        return wrap(snapshot.get(data.row, columns));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public Map<Column, Value> columns(Column... columns) {
      try {
        return wrap(snapshot.get(data.row, new HashSet<>(Arrays.asList(columns))));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * @since 1.0.0
   */
  public class MapConverter {
    private Collection<Bytes> rows;
    private Set<Column> columns;

    public MapConverter(Collection<Bytes> rows, Set<Column> columns) {
      this.rows = rows;
      this.columns = columns;
    }

    private Map<Bytes, Map<Column, Bytes>> getInput() {
      try {
        return snapshot.get(rows, columns);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Map wrap2(Map m) {
      return Collections.unmodifiableMap(DefaultedMap.decorate(m, new DefaultedMap(new Value(
          (Bytes) null))));
    }

    @SuppressWarnings("unchecked")
    public Map<String, Map<Column, Value>> toStringMap() {
      Map<Bytes, Map<Column, Bytes>> in = getInput();
      Map<String, Map<Column, Value>> out = new HashMap<>();

      for (Entry<Bytes, Map<Column, Bytes>> rowEntry : in.entrySet()) {
        out.put(encoder.decodeString(rowEntry.getKey()), wrap(rowEntry.getValue()));
      }

      return wrap2(out);
    }

    @SuppressWarnings("unchecked")
    public Map<Long, Map<Column, Value>> toLongMap() {
      Map<Bytes, Map<Column, Bytes>> in = getInput();
      Map<Long, Map<Column, Value>> out = new HashMap<>();

      for (Entry<Bytes, Map<Column, Bytes>> rowEntry : in.entrySet()) {
        out.put(encoder.decodeLong(rowEntry.getKey()), wrap(rowEntry.getValue()));
      }

      return wrap2(out);
    }

    @SuppressWarnings("unchecked")
    public Map<Integer, Map<Column, Value>> toIntegerMap() {
      Map<Bytes, Map<Column, Bytes>> in = getInput();
      Map<Integer, Map<Column, Value>> out = new HashMap<>();

      for (Entry<Bytes, Map<Column, Bytes>> rowEntry : in.entrySet()) {
        out.put(encoder.decodeInteger(rowEntry.getKey()), wrap(rowEntry.getValue()));
      }

      return wrap2(out);
    }

    @SuppressWarnings("unchecked")
    public Map<Bytes, Map<Column, Value>> toBytesMap() {
      Map<Bytes, Map<Column, Bytes>> in = getInput();
      Map<Bytes, Map<Column, Value>> out = new HashMap<>();

      for (Entry<Bytes, Map<Column, Bytes>> rowEntry : in.entrySet()) {
        out.put(rowEntry.getKey(), wrap(rowEntry.getValue()));
      }

      return wrap2(out);
    }
  }

  /**
   * @since 1.0.0
   */
  public class ColumnsMethods {
    private Collection<Bytes> rows;

    public ColumnsMethods(Collection<Bytes> rows) {
      this.rows = rows;
    }

    public MapConverter columns(Set<Column> columns) {
      return new MapConverter(rows, columns);
    }

    public MapConverter columns(Column... columns) {
      return columns(new HashSet<>(Arrays.asList(columns)));
    }
  }

  /**
   * @since 1.0.0
   */
  public class ValueRowMethods extends RowMethods<ValueFamilyMethods> {

    ValueRowMethods() {
      tl.super();
    }

    @Override
    ValueFamilyMethods create(Data data) {
      return new ValueFamilyMethods(data);
    }

    public ColumnsMethods rows(Collection<Bytes> rows) {
      return new ColumnsMethods(rows);
    }

    public ColumnsMethods rows(Bytes... rows) {
      return new ColumnsMethods(Arrays.asList(rows));
    }

    public ColumnsMethods rowsString(String... rows) {
      return rowsString(Arrays.asList(rows));
    }

    public ColumnsMethods rowsString(Collection<String> rows) {
      ArrayList<Bytes> conv = new ArrayList<>();
      for (String row : rows) {
        conv.add(encoder.encode(row));
      }

      return rows(conv);
    }

    public ColumnsMethods rowsLong(Long... rows) {
      return rowsLong(Arrays.asList(rows));
    }

    public ColumnsMethods rowsLong(Collection<Long> rows) {
      ArrayList<Bytes> conv = new ArrayList<>();
      for (Long row : rows) {
        conv.add(encoder.encode(row));
      }

      return rows(conv);
    }

    public ColumnsMethods rowsInteger(Integer... rows) {
      return rowsInteger(Arrays.asList(rows));
    }

    public ColumnsMethods rowsInteger(Collection<Integer> rows) {
      ArrayList<Bytes> conv = new ArrayList<>();
      for (Integer row : rows) {
        conv.add(encoder.encode(row));
      }

      return rows(conv);
    }

    public ColumnsMethods rowsBytes(byte[]... rows) {
      return rowsBytes(Arrays.asList(rows));
    }

    public ColumnsMethods rowsBytes(Collection<byte[]> rows) {
      ArrayList<Bytes> conv = new ArrayList<>();
      for (byte[] row : rows) {
        conv.add(Bytes.of(row));
      }

      return rows(conv);
    }

    public ColumnsMethods rowsByteBuffers(ByteBuffer... rows) {
      return rowsByteBuffers(Arrays.asList(rows));
    }

    public ColumnsMethods rowsByteBuffers(Collection<ByteBuffer> rows) {
      ArrayList<Bytes> conv = new ArrayList<>();
      for (ByteBuffer row : rows) {
        conv.add(Bytes.of(row));
      }

      return rows(conv);
    }

  }

  TypedSnapshotBase(SnapshotBase snapshot, Encoder encoder, TypeLayer tl) {
    this.snapshot = snapshot;
    this.encoder = encoder;
    this.tl = tl;
  }

  @Override
  public Bytes get(Bytes row, Column column) {
    return snapshot.get(row, column);
  }

  @Override
  public Map<Column, Bytes> get(Bytes row, Set<Column> columns) {
    return snapshot.get(row, columns);
  }

  @Override
  public Map<Bytes, Map<Column, Bytes>> get(Collection<RowColumn> rowColumns) {
    return snapshot.get(rowColumns);
  }

  @Override
  public RowIterator get(ScannerConfiguration config) {
    return snapshot.get(config);
  }

  @Override
  public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns) {
    return snapshot.get(rows, columns);
  }

  public ValueRowMethods get() {
    return new ValueRowMethods();
  }

  @SuppressWarnings({"unchecked"})
  private Map<Column, Value> wrap(Map<Column, Bytes> map) {
    Map<Column, Value> ret = Maps.transformValues(map, new Function<Bytes, Value>() {
      @Override
      public Value apply(Bytes input) {
        return new Value(input);
      }
    });

    return Collections.unmodifiableMap(DefaultedMap.decorate(ret, new Value((Bytes) null)));
  }

  @Override
  public long getStartTimestamp() {
    return snapshot.getStartTimestamp();
  }

  @Override
  public String gets(String row, Column column) {
    return snapshot.gets(row, column);
  }

  @Override
  public Map<Column, String> gets(String row, Set<Column> columns) {
    return snapshot.gets(row, columns);
  }

  @Override
  public Map<String, Map<Column, String>> gets(Collection<String> rows, Set<Column> columns) {
    return snapshot.gets(rows, columns);
  }

  @Override
  public Map<String, Map<Column, String>> gets(Collection<RowColumn> rowColumns) {
    return snapshot.gets(rowColumns);
  }
}
