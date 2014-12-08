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
package io.fluo.api.data;

import com.google.common.base.Preconditions;

/**
 * Represents all or a subset of the column family, column qualifier, and column visibility fields. A column with no fields set is represented by
 * {@link Column.EMPTY}. Column is immutable after it is created.
 */
public class Column {
  
  public static final Bytes UNSET = Bytes.wrap(new byte[0]);
  
  private Bytes family = UNSET;
  private Bytes qualifier = UNSET;
  private Bytes visibility = UNSET;
  
  public static final Column EMPTY = new Column();

  /**
   * Creates an empty Column where family, qualifier and visibility are not set
   */
  public Column() {}
  
  /**
   * Creates Column with only a family.
   */
  public Column(Bytes family) {
    Preconditions.checkNotNull(family, "Family must not be null");
    this.family = family;
  }
  
  /**
   * Creates Column with only a family. String parameter will be encoded as UTF-8.
   */
  public Column(String family) {
    this(family == null ? null : Bytes.wrap(family));
  }

  /**
   * Creates Column with a family and qualifier.
   */
  public Column(Bytes family, Bytes qualifier) {
    Preconditions.checkNotNull(family, "Family must not be null");
    Preconditions.checkNotNull(qualifier, "Qualifier must not be null");
    this.family = family;
    this.qualifier = qualifier;
  }
  
  /**
   * Creates Column with a family and qualifier.  String parameters will be encoded as UTF-8.
   */
  public Column(String family, String qualifier) {
    this(family == null ? null : Bytes.wrap(family), 
         qualifier == null ? null : Bytes.wrap(qualifier));
  }

  /** 
   * Creates Column with family, qualifier, and visibility
   */
  public Column(Bytes family, Bytes qualifier, Bytes visibility) {
    Preconditions.checkNotNull(family, "Family must not be null");
    Preconditions.checkNotNull(qualifier, "Qualifier must not be null");
    Preconditions.checkNotNull(visibility, "Visibility must not be null");
    this.family = family;
    this.qualifier = qualifier;
    this.visibility = visibility;
  }
  
  /** 
   * Creates Column with family, qualifier, and visibility. String parameters will be encoded as UTF-8.
   */
  public Column(String family, String qualifier, String visibility) {
    this(family == null ? null : Bytes.wrap(family), 
         qualifier == null ? null : Bytes.wrap(qualifier), 
         visibility == null ? null : Bytes.wrap(visibility));
  }
  
  /**
   * Returns true if family is set
   */
  public boolean isFamilySet() {
    return family != UNSET;
  }
  
  /**
   * Retrieves Column Family (in Bytes).  Returns {@link Bytes.EMPTY} if not set.
   */
  public Bytes getFamily() {
    if (!isFamilySet()) {
      return Bytes.EMPTY;
    }
    return family;
  }
  
  /**
   * Returns true if qualifier is set
   */
  public boolean isQualifierSet() {
    return qualifier != UNSET;
  }
  
  /**
   * Retrieves Column Qualifier (in Bytes). Returns {@link Bytes.EMPTY} if not set.
   */
  public Bytes getQualifier() {
    if (!isQualifierSet()) {
      return Bytes.EMPTY;
    }
    return qualifier;
  }
  
  /**
   * Returns true if visibility is set. 
   */
  public boolean isVisibilitySet() {
    return visibility != UNSET;
  }
  
  /**
   * Retrieves Column Visibility (in Bytes). Returns {@link Bytes.EMPTY} if not set.
   */
  public Bytes getVisibility() {
    if (!isVisibilitySet()) {
      return Bytes.EMPTY;
    }
    return visibility;
  }
  
  @Override
  public String toString() {
    return family + " " + qualifier + " " + visibility;
  }
  
  @Override
  public int hashCode() {
    return family.hashCode() + qualifier.hashCode() + visibility.hashCode();
  }
  
  @Override
  public boolean equals(Object o) {    
    if (o instanceof Column) {
      Column oc = (Column) o;
      return family.equals(oc.getFamily()) && qualifier.equals(oc.getQualifier()) && visibility.equals(oc.getVisibility());
    }
    return false;
  }
}
