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

package org.apache.fluo.api.data;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a range between two {@link RowColumn}s in a Fluo table. Similar to an Accumulo Range.
 * Span is immutable after it is created.
 *
 * @since 1.0.0
 */
public final class Span implements Serializable {

  private static final long serialVersionUID = 1L;
  private RowColumn start = RowColumn.EMPTY;
  private boolean startInclusive = true;
  private RowColumn end = RowColumn.EMPTY;
  private boolean endInclusive = true;

  /**
   * Constructs a span with infinite start and end
   */
  public Span() {}

  /**
   * Construct a new Range using a builder class
   *
   * @param builder Builder object
   */
  private Span(Builder builder) {
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
   * Construct a new span from a start and end RowColumn. Set either key to RowColumn.EMPTY to
   * indicate positive or negative infinite
   *
   * @param start Start RowColumn
   * @param startInclusive Include start in Range
   * @param end End RowColumn
   * @param endInclusive Include end in Range
   */
  public Span(RowColumn start, boolean startInclusive, RowColumn end, boolean endInclusive) {
    Objects.requireNonNull(start, "start must not be null");
    Objects.requireNonNull(end, "end must not be null");
    this.start = start;
    this.startInclusive = startInclusive;
    this.end = end;
    this.endInclusive = endInclusive;
  }

  /**
   * Construct a new Span from a start and end row. Set either row to Bytes.EMPTY to indicate
   * positive or negative infinite.
   *
   * @param startRow Start row
   * @param startRowInclusive Start row inclusive
   * @param endRow End row
   * @param endRowInclusive End row inclusive
   */
  public Span(Bytes startRow, boolean startRowInclusive, Bytes endRow, boolean endRowInclusive) {
    Objects.requireNonNull(startRow, "startRow must not be null");
    Objects.requireNonNull(endRow, "endRow must not be null");
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
   * Construct a new Span from a start and end row. Strings will be encoded as UTF-8.
   *
   * @param startRow Start row
   * @param startRowInclusive Start row inclusive
   * @param endRow End row
   * @param endRowInclusive End row inclusive
   */
  public Span(CharSequence startRow, boolean startRowInclusive, CharSequence endRow,
      boolean endRowInclusive) {
    this(Bytes.of(startRow), startRowInclusive, Bytes.of(endRow), endRowInclusive);
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
    return o instanceof Span && equals((Span) o);
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
    return ((startInclusive && !start.equals(RowColumn.EMPTY)) ? "[" : "(")
        + (start.equals(RowColumn.EMPTY) ? "-inf" : start) + ","
        + (end.equals(RowColumn.EMPTY) ? "+inf" : end)
        + ((endInclusive && !end.equals(RowColumn.EMPTY)) ? "]" : ")");
  }

  @Override
  public int hashCode() {
    return Objects.hash(start, end, startInclusive, endInclusive);
  }

  /**
   * Creates a span that covers an exact row
   */
  public static Span exact(Bytes row) {
    Objects.requireNonNull(row);
    return new Span(row, true, row, true);
  }

  /**
   * Creates a Span that covers an exact row. String parameters will be encoded as UTF-8
   */
  public static Span exact(CharSequence row) {
    Objects.requireNonNull(row);
    return exact(Bytes.of(row));
  }

  /**
   * Creates a Span that covers an exact row and {@link Column}. The {@link Column} passed to this
   * method can be constructed without a qualifier or visibility to create a Span at the family or
   * qualifier level.
   */
  public static Span exact(Bytes row, Column col) {
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);
    RowColumn start = new RowColumn(row, col);
    return new Span(start, true, start.following(), false);
  }

  /**
   * Creates a Span that covers an exact row and {@link Column}. The {@link Column} passed to this
   * method can be constructed without a qualifier or visibility to create a Span at the family or
   * qualifier level. String parameters will be encoded as UTF-8
   */
  public static Span exact(CharSequence row, Column col) {
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);
    return exact(Bytes.of(row), col);
  }

  private static Bytes followingPrefix(Bytes prefix) {
    byte[] prefixBytes = prefix.toArray();

    // find the last byte in the array that is not 0xff
    int changeIndex = prefix.length() - 1;
    while (changeIndex >= 0 && prefixBytes[changeIndex] == (byte) 0xff) {
      changeIndex--;
    }
    if (changeIndex < 0) {
      return null;
    }

    // copy prefix bytes into new array
    byte[] newBytes = new byte[changeIndex + 1];
    System.arraycopy(prefixBytes, 0, newBytes, 0, changeIndex + 1);

    // increment the selected byte
    newBytes[changeIndex]++;
    return Bytes.of(newBytes);
  }

  /**
   * Returns a Span that covers all rows beginning with a prefix.
   */
  public static Span prefix(Bytes rowPrefix) {
    Objects.requireNonNull(rowPrefix);
    Bytes fp = followingPrefix(rowPrefix);
    return new Span(rowPrefix, true, fp == null ? Bytes.EMPTY : fp, false);
  }

  /**
   * Returns a Span that covers all rows beginning with a prefix String parameters will be encoded
   * as UTF-8
   */
  public static Span prefix(CharSequence rowPrefix) {
    Objects.requireNonNull(rowPrefix);
    return prefix(Bytes.of(rowPrefix));
  }

  /**
   * Returns a Span that covers all columns beginning with a row and {@link Column} prefix. The
   * {@link Column} passed to this method can be constructed without a qualifier or visibility to
   * create a prefix Span at the family or qualifier level.
   */
  public static Span prefix(Bytes row, Column colPrefix) {
    Objects.requireNonNull(row);
    Objects.requireNonNull(colPrefix);
    Bytes cf = colPrefix.getFamily();
    Bytes cq = colPrefix.getQualifier();
    Bytes cv = colPrefix.getVisibility();

    if (colPrefix.isVisibilitySet()) {
      Bytes fp = followingPrefix(cv);
      RowColumn end = (fp == null ? new RowColumn(row, new Column(cf, cq)).following()
          : new RowColumn(row, new Column(cf, cq, fp)));
      return new Span(new RowColumn(row, colPrefix), true, end, false);
    } else if (colPrefix.isQualifierSet()) {
      Bytes fp = followingPrefix(cq);
      RowColumn end = (fp == null ? new RowColumn(row, new Column(cf)).following()
          : new RowColumn(row, new Column(cf, fp)));
      return new Span(new RowColumn(row, colPrefix), true, end, false);
    } else if (colPrefix.isFamilySet()) {
      Bytes fp = followingPrefix(cf);
      RowColumn end =
          (fp == null ? new RowColumn(row).following() : new RowColumn(row, new Column(fp)));
      return new Span(new RowColumn(row, colPrefix), true, end, false);
    } else {
      return prefix(row);
    }
  }

  /**
   * Returns a Span that covers all columns beginning with a row and {@link Column} prefix. The
   * {@link Column} passed to this method can be constructed without a qualifier or visibility to
   * create a prefix Span at the family or qualifier level. String parameters will be encoded as
   * UTF-8
   */
  public static Span prefix(CharSequence row, Column colPrefix) {
    Objects.requireNonNull(row);
    Objects.requireNonNull(colPrefix);
    return prefix(Bytes.of(row), colPrefix);
  }

  private static class KeyBuilder {
    protected Bytes row = Bytes.EMPTY;
    protected Bytes cf = Bytes.EMPTY;
    protected Bytes cq = Bytes.EMPTY;
    protected Bytes cv = Bytes.EMPTY;
    protected boolean inclusive = true;
    protected boolean infinite = true;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * @since 1.0.0
   */
  public static class Builder {

    private KeyBuilder start = new KeyBuilder();
    private KeyBuilder end = new KeyBuilder();

    private Builder() {}

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
    public StartCFBuilder startRow(CharSequence row) {
      return startRow(Bytes.of(row));
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
    public EndCFBuilder endRow(CharSequence row) {
      return endRow(Bytes.of(row));
    }

    public Span build() {
      return new Span(this);
    }
  }

  /**
   * @since 1.0.0
   */
  public static class StartBuilder {

    protected Builder builder;

    private StartBuilder(Builder builder) {
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
    public EndCFBuilder endRow(CharSequence row) {
      return endRow(Bytes.of(row));
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

  /**
   * @since 1.0.0
   */
  public static class EndBuilder {

    protected Builder builder;

    private EndBuilder(Builder builder) {
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

  /**
   * @since 1.0.0
   */
  public static class StartCVBuilder extends StartBuilder {

    private StartCVBuilder(Builder builder) {
      super(builder);
    }

    /**
     * Add column visibility to Span start
     */
    public StartBuilder vis(Bytes cv) {
      this.builder.start.cv = cv;
      return new StartBuilder(this.builder);
    }

    /**
     * Add column visibility (will be encoded UTF-8) to Span start
     */
    public StartBuilder vis(CharSequence cv) {
      return vis(Bytes.of(cv));
    }
  }

  /**
   * @since 1.0.0
   */
  public static class StartCQBuilder extends StartBuilder {

    private StartCQBuilder(Builder builder) {
      super(builder);
    }

    /**
     * Add column qualifier to Span start
     */
    public StartCVBuilder qual(Bytes cq) {
      this.builder.start.cq = cq;
      return new StartCVBuilder(this.builder);
    }

    /**
     * Add column qualifier (will be encoded UTF-8) to Span start
     */
    public StartCVBuilder qual(CharSequence cq) {
      return qual(Bytes.of(cq));
    }
  }

  /**
   * @since 1.0.0
   */
  public static class StartCFBuilder extends StartBuilder {

    private StartCFBuilder(Builder builder) {
      super(builder);
    }

    /**
     * Add column family to Span start
     */
    public StartCQBuilder fam(Bytes cf) {
      this.builder.start.cf = cf;
      return new StartCQBuilder(this.builder);
    }

    /**
     * Add column family (will be encoded UTF-8) to Span start
     */
    public StartCQBuilder fam(CharSequence cf) {
      return fam(Bytes.of(cf));
    }
  }

  /**
   * @since 1.0.0
   */
  public static class EndCVBuilder extends EndBuilder {

    private EndCVBuilder(Builder builder) {
      super(builder);
    }

    /**
     * Add column visibility to Span end
     */
    public EndBuilder vis(Bytes cv) {
      this.builder.end.cv = cv;
      return this;
    }

    /**
     * Add column visibility (will be encoded UTF-8) to Span end
     */
    public EndBuilder vis(CharSequence cv) {
      return vis(Bytes.of(cv));
    }
  }

  /**
   * @since 1.0.0
   */
  public static class EndCQBuilder extends EndBuilder {

    private EndCQBuilder(Builder builder) {
      super(builder);
    }

    /**
     * Add column qualifier to Span end
     */
    public EndCVBuilder qual(Bytes cq) {
      this.builder.end.cq = cq;
      return new EndCVBuilder(this.builder);
    }

    /**
     * Add column qualifier (will be encoded UTF-8) to Span end
     */
    public EndCVBuilder qual(CharSequence cq) {
      return qual(Bytes.of(cq));
    }
  }

  /**
   * @since 1.0.0
   */
  public static class EndCFBuilder extends EndBuilder {

    private EndCFBuilder(Builder builder) {
      super(builder);
    }

    /**
     * Add column family to an Span end
     */
    public EndCQBuilder fam(Bytes cf) {
      this.builder.end.cf = cf;
      return new EndCQBuilder(this.builder);
    }

    /**
     * Add column family (will be encoded UTF-8) to an Span end
     */
    public EndCQBuilder fam(CharSequence cf) {
      return fam(Bytes.of(cf));
    }
  }
}
