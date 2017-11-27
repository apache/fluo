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

import org.apache.fluo.api.data.Bytes;
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
   *
   * @since 1.2.0
   * @see org.apache.fluo.api.data.Span#exact(Bytes)
   * @param row restrict the scanner to data in an exact row
   * @return self
   */
  default ScannerBuilder over(Bytes row) {
    return over(Span.exact(row));
  }

  /**
   *
   * @since 1.2.0
   * @see org.apache.fluo.api.data.Span#exact(CharSequence)
   * @param row restrict the scanner to data in an exact row. String parameters will be encoded as
   *        UTF-8
   * @return self
   */
  default ScannerBuilder over(CharSequence row) {
    return over(Span.exact(row));
  }

  /**
   *
   * @see org.apache.fluo.api.data.Span#exact(Bytes, Column)
   * @param row restrict the scanner to data in an exact row
   * @param col restrict the scanner to data in exact {@link org.apache.fluo.api.data.Column}.
   * @since 1.2.0
   */
  default ScannerBuilder over(Bytes row, Column col) {
    return over(Span.exact(row, col));
  }

  /**
   *
   * @see org.apache.fluo.api.data.Span#exact(CharSequence, Column)
   * @param row restrict the scanner to data in an exact row
   * @param col restrict the scanner to data in exact {@link org.apache.fluo.api.data.Column}.
   * @return self
   * @since 1.2.0
   */
  default ScannerBuilder over(CharSequence row, Column col) {
    return over(Span.exact(row, col));
  }

  /**
   *
   * @see org.apache.fluo.api.data.Span#prefix(Bytes)
   * @param rowPrefix restrict the scanner to data in rows that begins with a prefix
   * @return self
   * @since 1.2.0
   */
  default ScannerBuilder overPrefix(Bytes rowPrefix) {
    return over(Span.prefix(rowPrefix));
  }

  /**
   *
   * @see org.apache.fluo.api.data.Span#prefix(CharSequence)
   * @param rowPrefix restrict the scanner to data in rows that begins with a prefix.
   * @return self
   * @since 1.2.0
   */
  default ScannerBuilder overPrefix(CharSequence rowPrefix) {
    return over(Span.prefix(rowPrefix));
  }

  /**
   *
   * @see org.apache.fluo.api.data.Span#prefix(Bytes, Column)
   * @param row restrict the scanner to data in an exact row.
   * @param colPrefix restrict scanner to data that begins with specifiec
   *        {@link org.apache.fluo.api.data.Column} prefix.
   * @return self
   * @since 1.2.0
   */
  default ScannerBuilder overPrefix(Bytes row, Column colPrefix) {
    return over(Span.prefix(row, colPrefix));
  }

  /**
   *
   * @see org.apache.fluo.api.data.Span#prefix(CharSequence, Column)
   * @param row restrict the scanner to data in an exact row.
   * @param colPrefix restrict scanner to data that begins with specifiec
   *        {@link org.apache.fluo.api.data.Column} prefix.
   * @return self
   * @since 1.2.0
   */
  default ScannerBuilder overPrefix(CharSequence row, Column colPrefix) {
    return over(Span.prefix(row, colPrefix));
  }

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
