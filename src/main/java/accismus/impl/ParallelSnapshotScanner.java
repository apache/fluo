package accismus.impl;

import java.util.ArrayList;
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

import accismus.api.Column;
import accismus.api.exceptions.StaleScanException;

public class ParallelSnapshotScanner {

  private Configuration aconfig;
  private long startTs;
  private List<ByteSequence> rows;
  private Set<Column> columns;

  ParallelSnapshotScanner(List<ByteSequence> rows, Set<Column> columns, Configuration aconfig, long startTs) {
    this.rows = rows;
    this.columns = columns;
    this.aconfig = aconfig;
    this.startTs = startTs;
  }

  private BatchScanner setupBatchScanner(List<ByteSequence> rows, Set<Column> columns) {
    BatchScanner scanner;
    try {
      // TODO hardcoded number of threads!
      scanner = aconfig.getConnector().createBatchScanner(aconfig.getTable(), aconfig.getAuthorizations(), 3);
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

    int count = 0;
    while (true) {
      List<Entry<Key,Value>> locks = new ArrayList<Entry<Key,Value>>();

      Map<ByteSequence,Map<Column,ByteSequence>> ret = scan(locks);

      if (locks.size() > 0) {

        if (count == 10) {
          throw new NotImplementedException("Parallel lock recovery");
        }

        // TODO get unique set of primary lock row/cols and then determine status of those... and then roll back or forward in batch
        // TODO only reread locked row/cols
        UtilWaitThread.sleep(1000);

        count++;

        continue;
      }

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
