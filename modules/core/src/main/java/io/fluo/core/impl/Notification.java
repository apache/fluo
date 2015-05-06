/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.core.impl;

import java.util.List;

import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.core.util.ByteUtil;
import io.fluo.core.util.ColumnUtil;
import io.fluo.core.util.Flutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;

public class Notification extends RowColumn {
  private long timestamp;

  public Notification(Bytes row, Column col, long ts) {
    super(row, col);
    this.timestamp = ts;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Flutation newDelete(Environment env) {
    return newDelete(env, getTimestamp());
  }

  public Flutation newDelete(Environment env, long ts) {
    Flutation m = new Flutation(env, getRow());
    ColumnVisibility cv = env.getSharedResources().getVisCache().getCV(getColumn());
    m.putDelete(ColumnConstants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(getColumn()), cv, ts);
    return m;
  }

  public static Notification from(Key k) {
    Bytes row = ByteUtil.toBytes(k.getRowData());
    List<Bytes> ca = Bytes.split(Bytes.of(k.getColumnQualifierData().toArray()));
    Column col = new Column(ca.get(0), ca.get(1));
    return new Notification(row, col, k.getTimestamp());
  }
}
