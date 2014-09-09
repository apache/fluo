/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.core.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.values.WriteValue;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.core.exceptions.StaleScanException;
import io.fluo.core.util.ByteUtil;
import io.fluo.core.util.UtilWaitThread;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

public class ParallelSnapshotScanner {

  private final Environment env;
  private final long startTs;
  private final HashSet<Bytes> unscannedRows;
  private final Set<Column> columns;
  private final TxStats stats;

  ParallelSnapshotScanner(Collection<Bytes> rows, Set<Column> columns, Environment env, long startTs, TxStats stats) {
    this.unscannedRows = new HashSet<>(rows);
    this.columns = Collections.unmodifiableSet(columns);
    this.env = env;
    this.startTs = startTs;
    this.stats = stats;
  }

  private BatchScanner setupBatchScanner(Collection<Bytes> rows, Set<Column> columns) {
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

    List<Range> ranges = new ArrayList<Range>(rows.size());

    for (Bytes row : rows) {
      ranges.add(Range.exact(ByteUtil.toText(row)));
    }

    scanner.setRanges(ranges);

    SnapshotScanner.setupScanner(scanner, new ArrayList<Column>(columns), startTs);

    return scanner;
  }

  Map<Bytes,Map<Column,Bytes>> scan() {

    long waitTime = SnapshotScanner.INITIAL_WAIT_TIME;
    long startTime = System.currentTimeMillis();

    Map<Bytes,Map<Column,Bytes>> ret = new HashMap<Bytes,Map<Column,Bytes>>();

    while (true) {
      List<Entry<Key,Value>> locks = new ArrayList<Entry<Key,Value>>();

      scan(ret, locks);

      if (locks.size() > 0) {

        boolean resolvedAll = LockResolver.resolveLocks(env, startTs, stats, locks, startTime);

        if (!resolvedAll) {
          UtilWaitThread.sleep(waitTime);
          stats.incrementLockWaitTime(waitTime);
          waitTime = Math.min(SnapshotScanner.MAX_WAIT_TIME, waitTime * 2);
        }
        // TODO, could only rescan the row/cols that were locked instead of just the entire row

        // retain the rows that were locked for future scans
        HashSet<Bytes> lockedRows = new HashSet<>();
        for (Entry<Key,Value> entry : locks) {
          lockedRows.add(ByteUtil.toBytes(entry.getKey().getRowData()));
        }

        unscannedRows.retainAll(lockedRows);

        continue;
      }

      for (Map<Column,Bytes> cols : ret.values())
        stats.incrementEntriesReturned(cols.size());

      return ret;
    }
  }

  void scan(Map<Bytes,Map<Column,Bytes>> ret, List<Entry<Key,Value>> locks) {

    BatchScanner bs = setupBatchScanner(unscannedRows, columns);
    try {
      for (Entry<Key,Value> entry : bs) {
        Bytes row = ByteUtil.toBytes(entry.getKey().getRowData());
        Bytes cf = ByteUtil.toBytes(entry.getKey().getColumnFamilyData());
        Bytes cq = ByteUtil.toBytes(entry.getKey().getColumnQualifierData());

        // TODO cache col vis
        Column col = new Column(cf, cq).setVisibility(new ColumnVisibility(entry
            .getKey().getColumnVisibilityData().toArray()));

        long colType = entry.getKey().getTimestamp() & ColumnConstants.PREFIX_MASK;

        if (colType == ColumnConstants.LOCK_PREFIX) {
          locks.add(entry);
        } else if (colType == ColumnConstants.DATA_PREFIX) {
          Map<Column,Bytes> cols = ret.get(row);
          if (cols == null) {
            cols = new HashMap<Column,Bytes>();
            ret.put(row, cols);
          }

          cols.put(col, Bytes.wrap(entry.getValue().get()));
        } else if (colType == ColumnConstants.WRITE_PREFIX) {
          if (WriteValue.isTruncated(entry.getValue().get())) {
            throw new StaleScanException();
          } else {
            throw new IllegalArgumentException();
          }
        } else {
          throw new IllegalArgumentException("Unexpected column type " + colType);
        }
      }
    } finally {
      bs.close();
    }
  }

}
