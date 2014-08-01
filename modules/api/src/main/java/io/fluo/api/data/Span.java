/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.api.data;

import com.google.common.base.Preconditions;

import io.fluo.api.data.impl.SpanUtil;
import io.fluo.api.data.impl.ByteUtil;
import org.apache.accumulo.core.data.Range;

/**
 * Used to specify a span between two row/columns in a Fluo table
 */
public class Span {
    
  private RowColumn start = RowColumn.EMPTY;
  private boolean startInclusive = true;
  private RowColumn end = RowColumn.EMPTY;
  private boolean endInclusive = true;
      
  /**
   * Constructs a span with infinite start & end
   */
  public Span() { }
  
  /**
   * Construct a new Range using a builder class
   * 
   * @param builder
   */
  public Span(Builder builder) {
    this.startInclusive = builder.start.inclusive;
    if (!builder.start.infinite) {
      start = buildRowColumn(builder.start);
      if (startInclusive == false) {
        start = start.following();
        startInclusive = true;
      }
    }
    this.endInclusive = builder.end.inclusive;
    if (!builder.end.infinite) {
      end = buildRowColumn(builder.end);
      if (endInclusive) {
        end = end.following();
        endInclusive = false;
      }
    }
  }
 
  private static RowColumn buildRowColumn(KeyBuilder key) {
    if (key.infinite || key.row.equals(Bytes.EMPTY)) {
      return RowColumn.EMPTY;
    } else if (key.cf.equals(Bytes.EMPTY)) {
      return new RowColumn(key.row);
    } else if (key.cq.equals(Bytes.EMPTY)) {
      return new RowColumn(key.row, new Column(key.cf));
    } else if (key.cv.equals(Bytes.EMPTY)) {
      return new RowColumn(key.row, new Column(key.cf, key.cq));
    }
    return new RowColumn(key.row, new Column(key.cf, key.cq, key.cv));
  }
  
  /**
   * Construct a new span from a start and end RowColumn.  Set either key
   * to RowColumn.EMPTY to indicate positive or negative infinite
   * 
   * @param startKey Start key
   * @param startKeyInclusive Include start key in Range
   * @param endKey End Key
   * @param endKeyInclusive Include end key in Range
   */
  public Span(RowColumn start, boolean startInclusive, RowColumn end, boolean endInclusive) {
    Preconditions.checkNotNull(start, "start must not be null");
    Preconditions.checkNotNull(end, "end must not be null");
    this.start = start;
    this.startInclusive = startInclusive;
    this.end = end;
    this.endInclusive = endInclusive;
  }
    
  /**
   * Construct a new Span from a start and end row
   * 
   * @param startRow Start row
   * @param startRowInclusive Start row inclusive
   * @param endRow End row
   * @param endRowInclusive End row inclusive
   */
  public Span(Bytes startRow, boolean startRowInclusive, Bytes endRow, boolean endRowInclusive) {
    Preconditions.checkNotNull(startRow, "startRow must not be null");
    Preconditions.checkNotNull(endRow, "endRow must not be null");
    this.startInclusive = startRowInclusive;
    if (!startRow.equals(Bytes.EMPTY)) {
      this.start = new RowColumn(startRow);
      if (startInclusive == false) {
        this.start = start.following();
        this.startInclusive = true;
      }
    }
    this.endInclusive = endRowInclusive;
    if (!endRow.equals(Bytes.EMPTY)) {
      this.end = new RowColumn(endRow);
      if (endInclusive) {
        end = end.following();
        endInclusive = false;
      }
    }
  }
  
  /**
   * Construct a new Span from a start and end row.
   * Strings will be encoded as UTF-8.
   * 
   * @param startRow Start row
   * @param startRowInclusive Start row inclusive
   * @param endRow End row
   * @param endRowInclusive End row inclusive
   */
  public Span(String startRow, boolean startRowInclusive, String endRow, boolean endRowInclusive) {
    this(Bytes.wrap(startRow), startRowInclusive, Bytes.wrap(endRow), endRowInclusive);
  }
  
  private Span(Range range) {
    this.start = SpanUtil.toRowColumn(range.getStartKey());
    this.startInclusive = range.isStartKeyInclusive();
    this.end = SpanUtil.toRowColumn(range.getEndKey());
    this.endInclusive = range.isEndKeyInclusive();
  }
  
  /**
   * Return start RowColumn of Span. 
   * 
   * @return start or RowColumn.EMPTY if infinite start
   */
  public RowColumn getStart() {
    return start;
  }
 
  /**
   * Return end RowColumn of Span
   * 
   * @return end or RowColumn.EMPTY if infinite end
   */
  public RowColumn getEnd() {
    return end;
  }
  
  /**
   * Checks if start RowColumn is inclusive
   * 
   * @return True if start key is inclusive
   */
  public boolean isStartInclusive() {
    return startInclusive;
  }

  /**
   * Check if end RowColumn is inclusive
   * 
   * @return True if end key is inclusive
   */
  public boolean isEndInclusive() {
    return endInclusive;
  }
        
  @Override
  public boolean equals(Object o) {
    if (o instanceof Span)
      return equals((Span) o);
    return false;
  }
  
  /**
   * Checks if span is equal to another span
   * 
   * @param other Span
   * @return true if equal
   */
  public boolean equals(Span other) {
    return start.equals(other.start) && (startInclusive == other.startInclusive) 
        && end.equals(other.end) && (endInclusive == other.endInclusive);
  }
  
  @Override
  public String toString() {
    return ((startInclusive && !start.equals(RowColumn.EMPTY)) ? "[" : "(") + (start.equals(RowColumn.EMPTY) ? "-inf" : start) + "," + 
        (end.equals(RowColumn.EMPTY) ? "+inf" : end) + ((endInclusive && !end.equals(RowColumn.EMPTY)) ? "]" : ")");
  }
  
  /**
   * Creates a span that covers an exact row
   */
  public static Span exact(Bytes row) {
    return new Span(Range.exact(ByteUtil.toText(row)));
  }
  
  /**
   * Creates a Span that covers an exact row.
   * String parameters will be encoded as UTF-8
   */
  public static Span exact(String row) {
    return exact(Bytes.wrap(row));
  }
  
  /**
   * Creates a Span that covers an exact row and column family
   */
  public static Span exact(Bytes row, Bytes cf) {
    return new Span(Range.exact(ByteUtil.toText(row), ByteUtil.toText(cf)));
  }
  
  /**
   * Creates a Span that covers an exact row and column family
   * String parameters will be encoded as UTF-8
   */
  public static Span exact(String row, String cf) {
    return exact(Bytes.wrap(row), Bytes.wrap(cf));
  }
  
  /**
   * Creates a Span that covers an exact row, column family, and column qualifier
   */
  public static Span exact(Bytes row, Bytes cf, Bytes cq) {
    return new Span(Range.exact(ByteUtil.toText(row), 
        ByteUtil.toText(cf), ByteUtil.toText(cq)));
  }
  
  /**
   * Creates a Span that covers an exact row, column family, and column qualifier
   * String parameters will be encoded as UTF-8
   */
  public static Span exact(String row, String cf, String cq) {
    return exact(Bytes.wrap(row), Bytes.wrap(cf), Bytes.wrap(cq));
  }
  
  /**
   * Creates a Span that covers an exact row, column family, column qualifier, and column visibility
   */
  public static Span exact(Bytes row, Bytes cf, Bytes cq, Bytes cv) {
    return new Span(Range.exact(ByteUtil.toText(row), 
        ByteUtil.toText(cf), ByteUtil.toText(cq), ByteUtil.toText(cv)));
  }
  
  /**
   * Creates a Span that covers an exact row, column family, column qualifier, and column visibility
   * String parameters will be encoded as UTF-8
   */
  public static Span exact(String row, String cf, String cq, String cv) {
    return exact(Bytes.wrap(row), Bytes.wrap(cf), Bytes.wrap(cq), Bytes.wrap(cv));
  }
  
  /**
   * Returns a Span that covers all rows beginning with a prefix
   */
  public static Span prefix(Bytes rowPrefix) {
    return new Span(Range.prefix(ByteUtil.toText(rowPrefix)));
  }
  
  /**
   * Returns a Span that covers all rows beginning with a prefix
   * String parameters will be encoded as UTF-8
   */
  public static Span prefix(String rowPrefix) {
    return prefix(Bytes.wrap(rowPrefix));
  }
  
  /**
   * Returns a Span that covers all column families beginning with a prefix within a given row
   */
  public static Span prefix(Bytes row, Bytes cfPrefix) {
    return new Span(Range.prefix(ByteUtil.toText(row), 
        ByteUtil.toText(cfPrefix)));
  }
  
  /**
   * Returns a Span that covers all column families beginning with a prefix within a given row
   * String parameters will be encoded as UTF-8
   */
  public static Span prefix(String row, String cfPrefix) {
    return prefix(Bytes.wrap(row), Bytes.wrap(cfPrefix));
  }
  
  /**
   * Returns a Span that covers all column qualifiers beginning with a prefix within a given row
   * and column family
   */
  public static Span prefix(Bytes row, Bytes cf, Bytes cqPrefix) {
    return new Span(Range.prefix(ByteUtil.toText(row), 
        ByteUtil.toText(cf), ByteUtil.toText(cqPrefix)));
  }
  
  /**
   * Returns a Span that covers all column qualifiers beginning with a prefix within a given row
   * String parameters will be encoded as UTF-8
   */
  public static Span prefix(String row, String cf, String cqPrefix) {
    return prefix(Bytes.wrap(row), Bytes.wrap(cf), Bytes.wrap(cqPrefix));
  }
  
  /**
   * Returns a Span that covers all column visibilities beginning with a prefix within a given row,
   * column family, and column qualifier.
   */
  public static Span prefix(Bytes row, Bytes cf, Bytes cq, Bytes cvPrefix) {
    return new Span(Range.prefix(ByteUtil.toText(row), 
        ByteUtil.toText(cf), ByteUtil.toText(cq), ByteUtil.toText(cvPrefix)));
  }
  
  /**
   * Returns a Span that covers all column visibilities beginning with a prefix within a given row
   * String parameters will be encoded as UTF-8
   */
  public static Span prefix(String row, String cf, String cq, String cvPrefix) {
    return prefix(Bytes.wrap(row), Bytes.wrap(cf), Bytes.wrap(cq), Bytes.wrap(cvPrefix));
  }
  
  public static class KeyBuilder {
    protected Bytes row = Bytes.EMPTY;
    protected Bytes cf = Bytes.EMPTY;
    protected Bytes cq = Bytes.EMPTY;
    protected Bytes cv = Bytes.EMPTY;
    protected boolean inclusive = true;
    protected boolean infinite = true;
  }
  
  public static class Builder {
    
    private KeyBuilder start = new KeyBuilder();
    private KeyBuilder end = new KeyBuilder();
    
    /**
     * Build start of Span starting with row
     */
    public StartCFBuilder startRow(Bytes row) {
      this.start.row = row;
      this.start.infinite = false;
      return new StartCFBuilder(this);
    }
    
    /**
     * Build start of Span starting with row (will be encoded UTF-8)
     */
    public StartCFBuilder startRow(String row) {
      return startRow(Bytes.wrap(row));
    }
    
    /**
     * Build end of Span starting with row
     */
    public EndCFBuilder endRow(Bytes row) {
      this.end.row = row;
      this.end.infinite = false;
      return new EndCFBuilder(this);
    }
    
    /**
     * Build end of Span starting with row (will be encoded UTF-8) 
     */
    public EndCFBuilder endRow(String row) {
      return endRow(Bytes.wrap(row));
    }
        
    public Span build() {
      return new Span(this);
    }
  }
    
  public static class StartBuilder {
    
    protected Builder builder;
    
    public StartBuilder(Builder builder) {
      this.builder = builder;
    }
    
    /**
     * Build Span end starting with row 
     */
    public EndCFBuilder endRow(Bytes row) {
      return this.builder.endRow(row);
    }
    
    /**
     * Build Span end starting with row (will be encoded UTF-8)
     */
    public EndCFBuilder endRow(String row) {
      return endRow(Bytes.wrap(row));
    }
    
    /**
     * Exclude start from Span
     */
    public StartBuilder exclusive() {
      this.builder.start.inclusive = false;
      return this;
    }
    
    public Span build() {
      return new Span(builder);
    }
  }
  
  public static class EndBuilder {
    
    protected Builder builder;
    
    public EndBuilder(Builder builder) {
      this.builder = builder;
    }
        
    /**
     * Exclude end from Span
     */
    public EndBuilder exclusive() {
      this.builder.end.inclusive = false;
      return this;
    }
    
    /**
     * Build Span
     */
    public Span build() {
      return new Span(builder);
    }
  }
    
  public static class StartCVBuilder extends StartBuilder {
    
    public StartCVBuilder(Builder builder) {
      super(builder);
    }
    
    /**
     * Add column visibility to Span start
     */
    public StartBuilder cv(Bytes cv) {
      this.builder.start.cv = cv;
      return new StartBuilder(this.builder);
    }
    
    /**
     * Add column visibility (will be encoded UTF-8) to Span start
     */
    public StartBuilder cv(String cv) {
      return cv(Bytes.wrap(cv));
    }
  }
  
  public static class StartCQBuilder extends StartBuilder {
    
    public StartCQBuilder(Builder builder) {
      super(builder);
    }
    
    /**
     * Add column qualifier to Span start
     */
    public StartCVBuilder cq(Bytes cq) {
      this.builder.start.cq = cq;
      return new StartCVBuilder(this.builder);
    }
    
    /**
     * Add column qualifier (will be encoded UTF-8) to Span start 
     */
    public StartCVBuilder cq(String cq) {
      return cq(Bytes.wrap(cq));
    }
  }
  
  public static class StartCFBuilder extends StartBuilder {
    
    public StartCFBuilder(Builder builder) {
      super(builder);
    }
    
    /**
     * Add column family to Span start
     */
    public StartCQBuilder cf(Bytes cf) {
      this.builder.start.cf = cf;
      return new StartCQBuilder(this.builder);
    }
    
    /**
     * Add column family (will be encoded UTF-8) to Span start
     */
    public StartCQBuilder cf(String cf) {
      return cf(Bytes.wrap(cf));
    }
  }
  
  public static class EndCVBuilder extends EndBuilder {
    
    public EndCVBuilder(Builder builder) {
      super(builder);
    }
    
    /**
     * Add column visibility to Span end
     */
    public EndBuilder cv(Bytes cv) {
      this.builder.end.cv = cv;
      return this;
    }
    
    /**
     * Add column visibility (will be encoded UTF-8) to Span end
     */
    public EndBuilder cv(String cv) {
      return cv(Bytes.wrap(cv));
    }
  }
  
  public static class EndCQBuilder extends EndBuilder {
    
    public EndCQBuilder(Builder builder) {
      super(builder);
    }
    
    /**
     * Add column qualifier to Span end
     */
    public EndCVBuilder cq(Bytes cq) {
      this.builder.end.cq = cq;
      return new EndCVBuilder(this.builder);
    }
    
    /**
     * Add column qualifier (will be encoded UTF-8) to Span end
     */
    public EndCVBuilder cq(String cq) {
      return cq(Bytes.wrap(cq));
    }
  }
  
  public static class EndCFBuilder extends EndBuilder {
    
    public EndCFBuilder(Builder builder) {
      super(builder);
    }
    
    /**
     * Add column family to an Span end
     */
    public EndCQBuilder cf(Bytes cf) {
      this.builder.end.cf = cf;
      return new EndCQBuilder(this.builder);
    }
    
    /**
     * Add column family (will be encoded UTF-8) to an Span end
     */
    public EndCQBuilder cf(String cf) {
      return cf(Bytes.wrap(cf));
    }
  }
}
