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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.core.impl.TransactionImpl;

public class AsyncReader {
  private PriorityQueue<AsyncGet> getsQueue;
  private ExecutorService executorService;
  private TransactionImpl tx;

  public AsyncReader(TransactionImpl tx) {
    this.tx = tx;
    getsQueue = new PriorityQueue<>();
    executorService = Executors.newSingleThreadExecutor();
  }

  public CompletableFuture<String> gets(String row, Column column) {
    return gets(row, column, null);
  }

  public CompletableFuture<String> gets(String row, Column column, String defaultValue) {
    AsyncGet curAsyncGet = new AsyncGet(row, column, defaultValue);
    getsQueue.add(curAsyncGet);
    executorService.submit(() -> {
      int i = 0;
      Collection<RowColumn> rowColumns = new HashSet<>();
      for (AsyncGet asyncGet : getsQueue) {
        rowColumns.add(asyncGet.rc);
      }
      Map<RowColumn, Bytes> getsMap = tx.get(rowColumns);

      while (!getsQueue.isEmpty()) {
        AsyncGet asyncGet = getsQueue.poll();
        Bytes bytesRes = getsMap.get(asyncGet.rc);
        asyncGet.res.complete(bytesRes == null ? defaultValue : bytesRes.toString());
      }
    });
    return curAsyncGet.res;
  }

  public void close() {

  }

  class AsyncGet {
    RowColumn rc;
    CompletableFuture<String> res;
    String defaultValue;

    public AsyncGet(String row, Column column, String defaultValue) {
      rc = new RowColumn(row, column);
      res = new CompletableFuture<>();
      this.defaultValue = defaultValue;
    }
  }
}
