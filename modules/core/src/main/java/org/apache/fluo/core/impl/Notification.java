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

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.fluo.accumulo.iterators.NotificationIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.Flutation;

import static org.apache.fluo.accumulo.util.NotificationUtil.decodeCol;
import static org.apache.fluo.accumulo.util.NotificationUtil.decodeTs;
import static org.apache.fluo.accumulo.util.NotificationUtil.encodeCol;
import static org.apache.fluo.accumulo.util.NotificationUtil.encodeTs;
import static org.apache.fluo.accumulo.util.NotificationUtil.isDelete;

/**
 * See {@link NotificationIterator} for explanation of notification timestamp serialization.
 *
 */
public class Notification {

  private final RowColumn rowCol;
  private final long timestamp;

  public Notification(Bytes row, Column col, long ts) {
    rowCol = new RowColumn(row, col);
    this.timestamp = ts;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Bytes getRow() {
    return rowCol.getRow();
  }

  public Column getColumn() {
    return rowCol.getColumn();
  }

  public RowColumn getRowColumn() {
    return rowCol;
  }

  public Flutation newDelete(Environment env) {
    return newDelete(env, getTimestamp());
  }

  public Flutation newDelete(Environment env, long ts) {
    Flutation m = new Flutation(env, rowCol.getRow());
    ColumnVisibility cv = env.getSharedResources().getVisCache().getCV(rowCol.getColumn());
    m.put(ColumnConstants.NOTIFY_CF.toArray(), encodeCol(rowCol.getColumn()), cv,
        encodeTs(ts, true), TransactionImpl.EMPTY);
    return m;
  }

  public static void put(Environment env, Mutation m, Column col, long ts) {
    ColumnVisibility cv = env.getSharedResources().getVisCache().getCV(col);
    m.put(ColumnConstants.NOTIFY_CF.toArray(), encodeCol(col), cv, encodeTs(ts, false),
        TransactionImpl.EMPTY);
  }

  public static Notification from(Key k) {
    Preconditions.checkArgument(!isDelete(k),
        "Method not expected to be used with delete notifications");
    Bytes row = ByteUtil.toBytes(k.getRowData());
    return new Notification(row, decodeCol(k), decodeTs(k));
  }

  public static void configureScanner(Scanner scanner) {
    scanner.fetchColumnFamily(ByteUtil.toText(ColumnConstants.NOTIFY_CF));
    scanner.addScanIterator(new IteratorSetting(11, NotificationIterator.class));
  }
}
