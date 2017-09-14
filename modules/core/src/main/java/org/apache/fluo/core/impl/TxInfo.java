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

package org.apache.fluo.core.impl;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.iterators.RollbackCheckIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.values.DelLockValue;
import org.apache.fluo.accumulo.values.WriteValue;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.core.util.ColumnUtil;

public class TxInfo {
  public TxStatus status = null;
  public long commitTs = -1;
  public byte[] lockValue = null;

  /**
   * determine the what state a transaction is in by inspecting the primary column
   */
  public static TxInfo getTransactionInfo(Environment env, Bytes prow, Column pcol, long startTs) {
    // TODO ensure primary is visible

    IteratorSetting is = new IteratorSetting(10, RollbackCheckIterator.class);
    RollbackCheckIterator.setLocktime(is, startTs);

    Entry<Key, Value> entry = ColumnUtil.checkColumn(env, is, prow, pcol);

    TxInfo txInfo = new TxInfo();

    if (entry == null) {
      txInfo.status = TxStatus.UNKNOWN;
      return txInfo;
    }

    long colType = entry.getKey().getTimestamp() & ColumnConstants.PREFIX_MASK;
    long ts = entry.getKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

    if (colType == ColumnConstants.LOCK_PREFIX) {
      if (ts == startTs) {
        txInfo.status = TxStatus.LOCKED;
        txInfo.lockValue = entry.getValue().get();
      } else {
        txInfo.status = TxStatus.UNKNOWN; // locked by another tx
      }
    } else if (colType == ColumnConstants.DEL_LOCK_PREFIX) {
      DelLockValue dlv = new DelLockValue(entry.getValue().get());

      if (ts != startTs) {
        // expect this to always be false, must be a bug in the iterator
        throw new IllegalStateException(prow + " " + pcol + " (" + ts + " != " + startTs + ") ");
      }

      if (dlv.isRollback()) {
        txInfo.status = TxStatus.ROLLED_BACK;
      } else {
        txInfo.status = TxStatus.COMMITTED;
        txInfo.commitTs = dlv.getCommitTimestamp();
      }
    } else if (colType == ColumnConstants.WRITE_PREFIX) {
      long timePtr = WriteValue.getTimestamp(entry.getValue().get());

      if (timePtr != startTs) {
        // expect this to always be false, must be a bug in the iterator
        throw new IllegalStateException(
            prow + " " + pcol + " (" + timePtr + " != " + startTs + ") ");
      }

      txInfo.status = TxStatus.COMMITTED;
      txInfo.commitTs = ts;
    } else {
      throw new IllegalStateException("unexpected col type returned " + colType);
    }

    return txInfo;
  }
}
