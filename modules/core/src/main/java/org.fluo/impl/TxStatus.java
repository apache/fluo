/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fluo.impl;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.mutable.MutableLong;

import org.fluo.api.Column;
import org.fluo.impl.iterators.RollbackCheckIterator;

public enum TxStatus {
  UNKNOWN, LOCKED, ROLLED_BACK, COMMITTED;
  
  /**
   * determine the what state a transaction is in by inspecting the primary column
   */
  public static TxStatus getTransactionStatus(Configuration config, ByteSequence prow, Column pcol, long startTs, MutableLong commitTs, Value lockVal) {
    // TODO ensure primary is visible

    IteratorSetting is = new IteratorSetting(10, RollbackCheckIterator.class);
    RollbackCheckIterator.setLocktime(is, startTs);
    
    Entry<Key,Value> entry = ColumnUtil.checkColumn(config, is, prow, pcol);

    TxStatus status;
    
    if (entry == null) {
      return TxStatus.UNKNOWN;
    }
    
    long colType = entry.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;
    long ts = entry.getKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
    
    if (colType == ColumnUtil.LOCK_PREFIX) {
      if (ts == startTs) {
        status = TxStatus.LOCKED;
        if (lockVal != null)
          lockVal.set(entry.getValue().get());
      } else
        status = TxStatus.UNKNOWN; // locked by another tx
    } else if (colType == ColumnUtil.DEL_LOCK_PREFIX) {
      DelLockValue dlv = new DelLockValue(entry.getValue().get());
      
      if (dlv.getTimestamp() != startTs) {
        // expect this to always be false, must be a bug in the iterator
        throw new IllegalStateException(prow + " " + pcol + " (" + dlv.getTimestamp() + " != " + startTs + ") ");
      }
      
      if (dlv.isRollback()) {
        status = TxStatus.ROLLED_BACK;
      } else {
        status = TxStatus.COMMITTED;
        commitTs.setValue(ts);
      }
    } else if (colType == ColumnUtil.WRITE_PREFIX) {
      long timePtr = WriteValue.getTimestamp(entry.getValue().get());
      
      if (timePtr != startTs) {
        // expect this to always be false, must be a bug in the iterator
        throw new IllegalStateException(prow + " " + pcol + " (" + timePtr + " != " + startTs + ") ");
      }

      status = TxStatus.COMMITTED;
      commitTs.setValue(ts);
    } else {
      throw new IllegalStateException("unexpected col type returned " + colType);
    }
    
    return status;
  }
}
