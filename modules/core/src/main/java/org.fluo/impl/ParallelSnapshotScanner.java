package org.fluo.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.commons.lang.NotImplementedException;

import org.fluo.api.Column;
import org.fluo.api.exceptions.StaleScanException;

public class ParallelSnapshotScanner {

  private Configuration aconfig;
  private long startTs;
  private Collection<ByteSequence> rows;
  private Set<Column> columns;
  private TxStats stats;

  ParallelSnapshotScanner(Collection<ByteSequence> rows, Set<Column> columns, Configuration aconfig, long startTs, TxStats stats) {
    this.rows = rows;
    this.columns = columns;
    this.aconfig = aconfig;
    this.startTs = startTs;
    this.stats = stats;
  }

  private BatchScanner setupBatchScanner(Collection<ByteSequence> rows, Set<Column> columns) {
    BatchScanner scanner;
    try {
      // TODO hardcoded number of threads!
      // one thread is probably good.. going for throughput
      scanner = aconfig.getConnector().createBatchScanner(aconfig.getTable(), aconfig.getAuthorizations(), 1);
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }

    scanner.clearColumns();
    scanner.clearScanIterators();

    List<Range> ranges = new ArrayList<Range>(rows.size());

    // TODO it may be better to create a range per row.... because scans between column qualifiers instead of seeking...

    for (ByteSequence row : rows) {
      ranges.add(Range.exact(ByteUtil.toText(row)));
    }

    scanner.setRanges(ranges);

    SnapshotScanner.setupScanner(scanner, new ArrayList<Column>(columns), startTs);

    return scanner;
  }


  Map<ByteSequence,Map<Column,ByteSequence>> scan() {

    long waitTime = SnapshotScanner.INITIAL_WAIT_TIME;
    long startTime = System.currentTimeMillis();

    while (true) {
      List<Entry<Key,Value>> locks = new ArrayList<Entry<Key,Value>>();

      Map<ByteSequence,Map<Column,ByteSequence>> ret = scan(locks);

      if (locks.size() > 0) {

        if (System.currentTimeMillis() - startTime > aconfig.getRollbackTime()) {
          throw new NotImplementedException("Parallel lock recovery");
        }

        // TODO get unique set of primary lock row/cols and then determine status of those... and then roll back or forward in batch
        // TODO only reread locked row/cols
        UtilWaitThread.sleep(waitTime);
        stats.incrementLockWaitTime(waitTime);
        waitTime = Math.min(SnapshotScanner.MAX_WAIT_TIME, waitTime * 2);

        // TODO only reread data that was locked when retrying, not everything

        continue;
      }

      for (Map<Column,ByteSequence> cols : ret.values())
        stats.incrementEntriesReturned(cols.size());

      return ret;
    }
  }

  Map<ByteSequence,Map<Column,ByteSequence>> scan(List<Entry<Key,Value>> locks) {

    Map<ByteSequence,Map<Column,ByteSequence>> ret = new HashMap<ByteSequence,Map<Column,ByteSequence>>();

    BatchScanner bs = setupBatchScanner(rows, columns);
    try {
      for (Entry<Key,Value> entry : bs) {
        ByteSequence row = entry.getKey().getRowData();

        // TODO cache col vis
        Column col = new Column(entry.getKey().getColumnFamilyData(), entry.getKey().getColumnQualifierData()).setVisibility(new ColumnVisibility(entry
            .getKey().getColumnVisibilityData().toArray()));

        long colType = entry.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;

        if (colType == ColumnUtil.LOCK_PREFIX) {
          locks.add(entry);
        } else if (colType == ColumnUtil.DATA_PREFIX) {
          Map<Column,ByteSequence> cols = ret.get(row);
          if (cols == null) {
            cols = new HashMap<Column,ByteSequence>();
            ret.put(row, cols);
          }

          cols.put(col, new ArrayByteSequence(entry.getValue().get()));
        } else if (colType == ColumnUtil.WRITE_PREFIX) {
          if (WriteValue.isTruncated(entry.getValue().get())) {
            throw new StaleScanException();
          } else {
            throw new IllegalArgumentException();
          }
        } else {
          throw new IllegalArgumentException();
        }
      }
    } finally {
      bs.close();
    }

    return ret;
  }

}
