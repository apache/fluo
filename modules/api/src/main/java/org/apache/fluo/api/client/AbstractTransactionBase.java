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
 * This class provides default implementations for many of the methods in TransactionBase. It exists
 * to make implementing TransactionBase easier.
 */

public abstract class AbstractTransactionBase extends AbstractSnapshotBase
    implements TransactionBase {

  public void delete(CharSequence row, Column col) {
    delete(s2bConv(row), col);
  }

  public void set(CharSequence row, Column col, CharSequence value) throws AlreadySetException {
    set(s2bConv(row), col, Bytes.of(value));
  }

  public void setWeakNotification(CharSequence row, Column col) {
    setWeakNotification(s2bConv(row), col);
  }
}
