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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

/**
 * Represents Column in Fluo.
 */
public class Column implements Writable {
  
  public static final Column EMPTY = new Column();
  
  private Bytes family = Bytes.EMPTY;
  private Bytes qualifier = Bytes.EMPTY;
  private Bytes visibility = Bytes.EMPTY;

  /**
   * Creates Column with family, qualifier and visibility
   * set to Bytes.EMPTY
   */
  public Column() {}
  
  /**
   * Creates Column with family and sets qualifier and visibility
   * to Bytes.EMPTY
   */
  public Column(Bytes family) {
    Preconditions.checkNotNull(family, "Family must not be null");
    this.family = family;
  }
  
  /**
   * Creates Column with family and sets qualifier and visibility 
   * to Bytes.EMPTY.  String parameter will be encoded as UTF-8.
   */
  public Column(String family) {
    this(family == null ? null : Bytes.wrap(family));
  }

  /**
   * Creates Column with family and qualifier and sets visibility
   * to Bytes.EMPTY
   */
  public Column(Bytes family, Bytes qualifier) {
    Preconditions.checkNotNull(family, "Family must not be null");
    Preconditions.checkNotNull(qualifier, "Qualifier must not be null");
    this.family = family;
    this.qualifier = qualifier;
  }
  
  /**
   * Creates Column with family and qualifier and sets visibility
   * to Bytes.EMPTY.  String parameters will be encoded as UTF-8.
   */
  public Column(String family, String qualifier) {
    this(family == null ? null : Bytes.wrap(family), qualifier == null ? null : Bytes.wrap(qualifier));
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
   * Creates Column with family, qualifier, and visibility.
   * String parameters will be encoded as UTF-8.
   */
  public Column(String family, String qualifier, String visibility) {
    this(family == null ? null : Bytes.wrap(family), qualifier == null ? null : Bytes.wrap(qualifier), 
        visibility == null ? null : Bytes.wrap(visibility));
  }
  
  /**
   * Retrieves Family of Column
   * 
   * @return Bytes Family 
   */
  public Bytes getFamily() {
    return family;
  }
  
  /**
   * Retrieves Qualifier of Column
   * 
   * @return Bytes Qualifier
   */
  public Bytes getQualifier() {
    return qualifier;
  }
  
  /**
   * Retrieves Visibility of Column
   * 
   * @return Bytes Visibility
   */
  public Bytes getVisibility() {
    return visibility;
  }

  /**
   * Sets visibility of Column
   * 
   */
  public Column setVisibility(String visibilty) {
    this.visibility = Bytes.wrap(visibilty);
    return this;
  }

  /**
   * Sets visibility of Column
   * 
   */
  public Column setVisibility(Bytes visibilty) {
    this.visibility = visibilty;
    return this;
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
      return family.equals(oc.family) && qualifier.equals(oc.qualifier)
          && visibility.equals(oc.visibility);
    }
    return false;
  }

  // TODO remove from public API
  public void write(DataOutput out) throws IOException {
    Bytes.write(out, family);
    Bytes.write(out, qualifier);
    Bytes.write(out, visibility);    
  }

  public void readFields(DataInput in) throws IOException {
    family = Bytes.read(in);
    qualifier = Bytes.read(in);
    visibility = Bytes.read(in);
  }
}
