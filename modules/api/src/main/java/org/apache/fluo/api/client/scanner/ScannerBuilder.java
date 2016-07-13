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

package org.apache.fluo.api.client.scanner;

import java.util.Collection;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.Span;

/**
 * @since 1.0.0
 */
public interface ScannerBuilder {
  /**
   * @param span restrict the scanner to data within span
   * @return self
   */
  ScannerBuilder over(Span span);


  /**
   * Passing in a Column with only the family set will fetch the entire column family.
   *
   * @param columns restrict the scanner to only these columns
   * @return self
   */
  ScannerBuilder fetch(Column... columns);

  /**
   * Passing in a Column with only the family set will fetch the entire column family.
   *
   * @param columns restrict the scanner to only these columns
   * @return self
   */
  ScannerBuilder fetch(Collection<Column> columns);

  /**
   * @return a new scanner created with any previously set restrictions
   */
  CellScanner build();

  /**
   * Call this to build a row scanner.
   *
   * @return a row scanner builder using any previously set restrictions
   */
  RowScannerBuilder byRow();
}
