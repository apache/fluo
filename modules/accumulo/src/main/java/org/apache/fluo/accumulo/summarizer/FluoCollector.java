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

package org.apache.fluo.accumulo.summarizer;

import org.apache.accumulo.core.client.summary.Summarizer.Collector;
import org.apache.accumulo.core.client.summary.Summarizer.StatisticConsumer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.util.ColumnType;
import org.apache.fluo.accumulo.util.NotificationUtil;
import org.apache.fluo.accumulo.util.ReadLockUtil;

public class FluoCollector implements Collector {

  private long ntfy = 0;
  private long ntfyDel = 0;
  private long txDone = 0;
  private long delLock = 0;
  private long lock = 0;
  private long data = 0;
  private long write = 0;
  private long ack = 0;
  private long delrlock = 0;
  private long rlock = 0;

  @Override
  public void accept(Key k, Value v) {

    if (NotificationUtil.isNtfy(k)) {
      if (NotificationUtil.isDelete(k)) {
        ntfyDel++;
      } else {
        ntfy++;
      }

    } else {
      ColumnType colType = ColumnType.from(k);
      switch (colType) {
        case TX_DONE:
          txDone++;
          break;
        case DEL_LOCK:
          delLock++;
          break;
        case LOCK:
          lock++;
          break;
        case DATA:
          data++;
          break;
        case WRITE:
          write++;
          break;
        case ACK:
          ack++;
          break;
        case RLOCK:
          if (ReadLockUtil.isDelete(k.getTimestamp())) {
            delrlock++;
          } else {
            rlock++;
          }
          break;
        default:
          throw new IllegalArgumentException("Unknown column type : " + colType);
      }
    }
  }

  @Override
  public void summarize(StatisticConsumer sc) {
    sc.accept("ntfy", ntfy);
    sc.accept("ntfyDel", ntfyDel);
    sc.accept("txDone", txDone);
    sc.accept("delLock", delLock);
    sc.accept("lock", lock);
    sc.accept("data", data);
    sc.accept("write", write);
    sc.accept("ack", ack);
    sc.accept("delrlock", delrlock);
    sc.accept("rlock", rlock);
  }
}
