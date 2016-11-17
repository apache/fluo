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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.CachedBytesConverter;
import org.apache.fluo.core.util.CachedColumnConverter;
import org.apache.fluo.core.util.ColumnUtil;
import org.apache.fluo.core.util.UtilWaitThread;

public class ParallelSnapshotScanner {

  private Environment env;
  private long startTs;
  private Collection<Bytes> rows;
  private Set<Column> columns;
  private TxStats stats;
  private List<Range> rangesToScan = new ArrayList<>();
  private Function<ByteSequence, Bytes> rowConverter;
  private Function<Key, Column> columnConverter;

  ParallelSnapshotScanner(Collection<Bytes> rows, Set<Column> columns, Environment env,
      long startTs, TxStats stats) {
    this.rows = rows;
    this.columns = columns;
    this.env = env;
    this.startTs = startTs;
    this.stats = stats;
    this.rowConverter = new CachedBytesConverter(rows);
    this.columnConverter = new CachedColumnConverter(columns);
  }

  ParallelSnapshotScanner(Collection<RowColumn> cells, Environment env, long startTs, TxStats stats) {
    for (RowColumn rc : cells) {
      byte[] r = rc.getRow().toArray();
      byte[] cf = rc.getColumn().getFamily().toArray();
      byte[] cq = rc.getColumn().getQualifier().toArray();
      byte[] cv = rc.getColumn().getVisibility().toArray();

      Key start = new Key(r, cf, cq, cv, Long.MAX_VALUE, false, false);
      Key end = new Key(start);
      end.setTimestamp(Long.MIN_VALUE);

      rangesToScan.add(new Range(start, true, end, true));
    }
    this.rows = null;
    this.env = env;
    this.startTs = startTs;
    this.stats = stats;
    this.rowConverter = ByteUtil::toBytes;
    this.columnConverter = ColumnUtil::convert;
  }

  private BatchScanner setupBatchScanner() {

    BatchScanner scanner;
    try {
      // TODO hardcoded number of threads!
      // one thread is probably good.. going for throughput
      scanner = env.getConnector().createBatchScanner(env.getTable(), env.getAuthorizations(), 1);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    if (rangesToScan.size() > 0) {
      scanner.setRanges(rangesToScan);
      SnapshotScanner.setupScanner(scanner, Collections.<Column>emptySet(), startTs);
    } else if (rows != null) {
      List<Range> ranges = new ArrayList<>(rows.size());

      for (Bytes row : rows) {
        ranges.add(Range.exact(ByteUtil.toText(row)));
      }

      scanner.setRanges(ranges);

      SnapshotScanner.setupScanner(scanner, columns, startTs);
    } else {
      return null;
    }

    return scanner;
  }

  Map<Bytes, Map<Column, Bytes>> scan() {

    long waitTime = SnapshotScanner.INITIAL_WAIT_TIME;
    long startTime = System.currentTimeMillis();

    Map<Bytes, Map<Column, Bytes>> ret = new HashMap<>();

    while (true) {
      List<Entry<Key, Value>> locks = new ArrayList<>();

      scan(ret, locks);

      if (locks.size() > 0) {

        boolean resolvedAll = LockResolver.resolveLocks(env, startTs, stats, locks, startTime);

        if (!resolvedAll) {
          UtilWaitThread.sleep(waitTime);
          stats.incrementLockWaitTime(waitTime);
          waitTime = Math.min(SnapshotScanner.MAX_WAIT_TIME, waitTime * 2);
        }

        // retain the rows that were locked for future scans
        rangesToScan.clear();
        rows = null;
        for (Entry<Key, Value> entry : locks) {
          Key start = new Key(entry.getKey());
          start.setTimestamp(Long.MAX_VALUE);
          Key end = new Key(entry.getKey());
          end.setTimestamp(Long.MIN_VALUE);
          rangesToScan.add(new Range(start, true, end, true));
        }

        continue;
      }

      for (Map<Column, Bytes> cols : ret.values()) {
        stats.incrementEntriesReturned(cols.size());
      }

      return ret;
    }
  }

  private void scan(Map<Bytes, Map<Column, Bytes>> ret, List<Entry<Key, Value>> locks) {

    BatchScanner bs = setupBatchScanner();
    try {
      for (Entry<Key, Value> entry : bs) {
        Bytes row = rowConverter.apply(entry.getKey().getRowData());
        Column col = columnConverter.apply(entry.getKey());

        long colType = entry.getKey().getTimestamp() & ColumnConstants.PREFIX_MASK;

        if (colType == ColumnConstants.LOCK_PREFIX) {
          locks.add(entry);
        } else if (colType == ColumnConstants.DATA_PREFIX) {
          Map<Column, Bytes> cols = ret.get(row);
          if (cols == null) {
            cols = new HashMap<>();
            ret.put(row, cols);
          }

          cols.put(col, Bytes.of(entry.getValue().get()));
        } else {
          throw new IllegalArgumentException("Unexpected column type " + colType);
        }
      }
    } finally {
      bs.close();
    }
  }

}
