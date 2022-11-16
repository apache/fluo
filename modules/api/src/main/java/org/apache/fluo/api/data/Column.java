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
 * Represents all or a subset of the column family, column qualifier, and column visibility fields.
 * A column with no fields set is represented by Column.EMPTY. Column is immutable after it is
 * created.
 *
 * @since 1.0.0
 */
public final class Column implements Comparable<Column>, Serializable {

  private static final long serialVersionUID = 1L;

  private final Bytes family;
  private final Bytes qualifier;
  private final Bytes visibility;

  private final boolean isFamilySet;
  private final boolean isQualifierSet;
  private final boolean isVisibilitySet;

  private int hashCode = 0;

  public static final Column EMPTY = new Column();

  /**
   * Creates an empty Column where family, qualifier and visibility are not set
   */
  public Column() {
    this.family = Bytes.EMPTY;
    this.isFamilySet = false;
    this.qualifier = Bytes.EMPTY;
    this.isQualifierSet = false;
    this.visibility = Bytes.EMPTY;
    this.isVisibilitySet = false;
  }

  /**
   * Creates Column with only a family.
   */
  public Column(Bytes family) {
    Objects.requireNonNull(family, "Family must not be null");
    this.family = family;
    this.isFamilySet = true;
    this.qualifier = Bytes.EMPTY;
    this.isQualifierSet = false;
    this.visibility = Bytes.EMPTY;
    this.isVisibilitySet = false;
  }

  /**
   * Creates Column with only a family. String parameter will be encoded as UTF-8.
   */
  public Column(CharSequence family) {
    this(Bytes.of(family));
  }

  /**
   * Creates Column with a family and qualifier.
   */
  public Column(Bytes family, Bytes qualifier) {
    Objects.requireNonNull(family, "Family must not be null");
    Objects.requireNonNull(qualifier, "Qualifier must not be null");
    this.family = family;
    this.isFamilySet = true;
    this.qualifier = qualifier;
    this.isQualifierSet = true;
    this.visibility = Bytes.EMPTY;
    this.isVisibilitySet = false;
  }

  /**
   * Creates Column with a family and qualifier. String parameters will be encoded as UTF-8.
   */
  public Column(CharSequence family, CharSequence qualifier) {
    this(Bytes.of(family), Bytes.of(qualifier));
  }

  /**
   * Creates Column with family, qualifier, and visibility
   */
  public Column(Bytes family, Bytes qualifier, Bytes visibility) {
    Objects.requireNonNull(family, "Family must not be null");
    Objects.requireNonNull(qualifier, "Qualifier must not be null");
    Objects.requireNonNull(visibility, "Visibility must not be null");
    this.family = family;
    this.isFamilySet = true;
    this.qualifier = qualifier;
    this.isQualifierSet = true;
    this.visibility = visibility;
    this.isVisibilitySet = true;
  }

  /**
   * Creates Column with family, qualifier, and visibility. String parameters will be encoded as
   * UTF-8.
   */
  public Column(CharSequence family, CharSequence qualifier, CharSequence visibility) {
    this(Bytes.of(family), Bytes.of(qualifier), Bytes.of(visibility));
  }

  /**
   * Returns true if family is set
   */
  public boolean isFamilySet() {
    return isFamilySet;
  }

  /**
   * Retrieves Column Family (in Bytes). Returns Bytes.EMPTY if not set.
   */
  public Bytes getFamily() {
    return family;
  }

  /**
   * Get the column family as a string using UTF-8 encoding.
   */
  public String getsFamily() {
    return getFamily().toString();
  }

  /**
   * Returns true if qualifier is set
   */
  public boolean isQualifierSet() {
    return isQualifierSet;
  }

  /**
   * Retrieves Column Qualifier (in Bytes). Returns Bytes.EMPTY if not set.
   */
  public Bytes getQualifier() {
    return qualifier;
  }

  /**
   * Get the column family as a string using UTF-8 encoding.
   */
  public String getsQualifier() {
    return getQualifier().toString();
  }

  /**
   * Returns true if visibility is set.
   */
  public boolean isVisibilitySet() {
    return isVisibilitySet;
  }

  /**
   * Retrieves Column Visibility (in Bytes). Returns Bytes.EMPTY if not set.
   */
  public Bytes getVisibility() {
    return visibility;
  }

  /**
   * Get the column visibility as a string using UTF-8 encoding.
   */
  public String getsVisibility() {
    return getVisibility().toString();
  }

  @Override
  public String toString() {
    return family + " " + qualifier + " " + visibility;
  }

  @Override
  public int hashCode() {
    if (hashCode == 0) {
      hashCode = Objects.hash(family, qualifier, visibility);
    }

    return hashCode;
  }

  @Override
  public int compareTo(Column other) {

    if (this == other) {
      return 0;
    }

    int result = family.compareTo(other.family);
    if (result == 0) {
      result = qualifier.compareTo(other.qualifier);
      if (result == 0) {
        result = visibility.compareTo(other.visibility);
      }
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {

    if (this == o) {
      return true;
    }

    if (o instanceof Column) {
      Column oc = (Column) o;

      return family.equals(oc.getFamily()) && qualifier.equals(oc.getQualifier())
          && visibility.equals(oc.getVisibility());
    }
    return false;
  }
}
