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

package org.apache.fluo.api.config;

import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.fluo.api.client.SnapshotBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;

/**
 * Contains configuration for a {@link org.apache.fluo.api.client.Snapshot} scanner. Passed to
 * {@link SnapshotBase#get(ScannerConfiguration)}.
 *
 * @since 1.0.0
 */
public class ScannerConfiguration implements Cloneable {

  private Span span = new Span();
  private Set<Column> columns = new HashSet<>();

  /**
   * Sets {@link Span} for ScannerConfiguration
   */
  public ScannerConfiguration setSpan(Span span) {
    Objects.requireNonNull(span);
    this.span = span;
    return this;
  }

  /**
   * Retrieves {@link Span} for ScannerConfiguration
   */
  public Span getSpan() {
    return span;
  }

  /**
   * List of all {@link Column}s that scanner will retrieve
   */
  public Set<Column> getColumns() {
    return Collections.unmodifiableSet(columns);
  }

  /**
   * Configures scanner to retrieve column with the given family
   */
  public ScannerConfiguration fetchColumnFamily(Bytes fam) {
    Objects.requireNonNull(fam);
    columns.add(new Column(fam));
    return this;
  }

  /**
   * Configures scanner to retrieve column with the given family and qualifier
   */
  public ScannerConfiguration fetchColumn(Bytes fam, Bytes qual) {
    Objects.requireNonNull(fam);
    Objects.requireNonNull(qual);
    columns.add(new Column(fam, qual));
    return this;
  }

  /**
   * Clears all fetched column settings
   */
  public void clearColumns() {
    columns.clear();
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object clone() throws CloneNotSupportedException {
    ScannerConfiguration sc = (ScannerConfiguration) super.clone();

    sc.columns = (Set<Column>) ((HashSet<Column>) columns).clone();
    sc.span = span;

    return sc;
  }
}
