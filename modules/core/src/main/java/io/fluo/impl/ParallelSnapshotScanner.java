package io.fluo.impl;

import io.fluo.api.Bytes;
import io.fluo.api.Column;
import io.fluo.api.exceptions.StaleScanException;
import io.fluo.core.util.UtilWaitThread;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.NotImplementedException;

public class ParallelSnapshotScanner {

  private Configuration aconfig;
  private long startTs;
  private Collection<Bytes> rows;
  private Set<Column> columns;
  private TxStats stats;

  ParallelSnapshotScanner(Collection<Bytes> rows, Set<Column> columns, Configuration aconfig, long startTs, TxStats stats) {
    this.rows = rows;
    this.columns = columns;
    this.aconfig = aconfig;
    this.startTs = startTs;
    this.stats = stats;
  }

  private BatchScanner setupBatchScanner(Collection<Bytes> rows, Set<Column> columns) {
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

    while (true) {
      List<Entry<Key,Value>> locks = new ArrayList<Entry<Key,Value>>();

      Map<Bytes,Map<Column,Bytes>> ret = scan(locks);

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

      for (Map<Column,Bytes> cols : ret.values())
        stats.incrementEntriesReturned(cols.size());

      return ret;
    }
  }

  Map<Bytes,Map<Column,Bytes>> scan(List<Entry<Key,Value>> locks) {

    Map<Bytes,Map<Column,Bytes>> ret = new HashMap<Bytes,Map<Column,Bytes>>();

    BatchScanner bs = setupBatchScanner(rows, columns);
    try {
      for (Entry<Key,Value> entry : bs) {
        Bytes row = Bytes.wrap(entry.getKey().getRowData().toArray());
        Bytes cf = Bytes.wrap(entry.getKey().getColumnFamilyData().toArray());
        Bytes cq = Bytes.wrap(entry.getKey().getColumnQualifierData().toArray());

        // TODO cache col vis
        Column col = new Column(cf, cq).setVisibility(new ColumnVisibility(entry
            .getKey().getColumnVisibilityData().toArray()));

        long colType = entry.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;

        if (colType == ColumnUtil.LOCK_PREFIX) {
          locks.add(entry);
        } else if (colType == ColumnUtil.DATA_PREFIX) {
          Map<Column,Bytes> cols = ret.get(row);
          if (cols == null) {
            cols = new HashMap<Column,Bytes>();
            ret.put(row, cols);
          }

          cols.put(col, Bytes.wrap(entry.getValue().get()));
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
