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

package org.apache.fluo.api.client;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.AlreadySetException;

/**
 * Enables users to read and write to a Fluo table at certain point in time. TransactionBase extends
 * {@link SnapshotBase} to include methods for writing to Fluo.
 *
 * @since 1.0.0
 * @see AbstractTransactionBase
 */
public interface TransactionBase extends SnapshotBase {

  /**
   * Deletes the value at the given row and {@link Column}
   */
  void delete(Bytes row, Column col);

  /**
   * Wrapper for {@link #delete(Bytes, Column)} that uses Strings. All String are encoded using
   * UTF-8.
   */
  void delete(CharSequence row, Column col);

  /**
   * Sets a value (in {@link Bytes}) at the given row and {@link Column}
   */
  void set(Bytes row, Column col, Bytes value) throws AlreadySetException;

  /**
   * Wrapper for {@link #set(Bytes, Column, Bytes)} that uses Strings. All String are encoded using
   * UTF-8.
   */
  void set(CharSequence row, Column col, CharSequence value) throws AlreadySetException;

  /**
   * Sets a weak notification at the given row and {@link Column}
   */
  void setWeakNotification(Bytes row, Column col);

  /**
   * Wrapper for {@link #setWeakNotification(Bytes, Column)} that uses Strings. All String are
   * encoded using UTF-8.
   */
  void setWeakNotification(CharSequence row, Column col);
}
