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

package org.apache.fluo.core.types;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.exceptions.AlreadySetException;

/**
 * A very simple implementation of {@link TransactionBase} used for testing. All reads are serviced
 * from {@link #getData}. Updates are stored in {@link #setData}, {@link #deletes}, or
 * {@link #weakNotifications} depending on the update type.
 */
public class MockTransactionBase extends MockSnapshotBase implements TransactionBase {

  final Map<Bytes, Map<Column, Bytes>> setData = new HashMap<>();
  final Map<Bytes, Set<Column>> deletes = new HashMap<>();
  final Map<Bytes, Set<Column>> weakNotifications = new HashMap<>();

  MockTransactionBase(String... entries) {
    super(entries);
  }

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    Set<Column> cols = weakNotifications.get(row);
    if (cols == null) {
      cols = new HashSet<>();
      weakNotifications.put(row, cols);
    }

    cols.add(col);
  }

  @Override
  public void set(Bytes row, Column col, Bytes value) {
    Map<Column, Bytes> cols = setData.get(row);
    if (cols == null) {
      cols = new HashMap<>();
      setData.put(row, cols);
    }

    cols.put(col, value);
  }

  @Override
  public void delete(Bytes row, Column col) {
    Set<Column> cols = deletes.get(row);
    if (cols == null) {
      cols = new HashSet<>();
      deletes.put(row, cols);
    }

    cols.add(col);
  }

  @Override
  public void setWeakNotification(String row, Column col) {
    setWeakNotification(Bytes.of(row), col);
  }

  @Override
  public void set(String row, Column col, String value) throws AlreadySetException {
    set(Bytes.of(row), col, Bytes.of(value));
  }

  @Override
  public void delete(String row, Column col) {
    delete(Bytes.of(row), col);
  }
}
