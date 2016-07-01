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

package org.apache.fluo.api.types;

import com.google.common.annotations.VisibleForTesting;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.exceptions.CommitException;

/**
 * A {@link Transaction} that uses a {@link TypeLayer}
 *
 * @since 1.0.0
 */
public class TypedTransaction extends TypedTransactionBase implements Transaction {

  private final Transaction closeTx;

  @VisibleForTesting
  protected TypedTransaction(Transaction tx, Encoder encoder, TypeLayer tl) {
    super(tx, encoder, tl);
    closeTx = tx;
  }

  @Override
  public void commit() throws CommitException {
    closeTx.commit();
  }

  @Override
  public void close() {
    closeTx.close();
  }
}
