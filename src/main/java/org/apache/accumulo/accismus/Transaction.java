package org.apache.accumulo.accismus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.accismus.impl.ColumnUtil;
import org.apache.accumulo.accismus.iterators.PrewriteIterator;
import org.apache.accumulo.accismus.iterators.SnapshotIterator;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.UtilWaitThread;
import org.apache.hadoop.io.Text;

import core.client.ConditionalWriter;
import core.client.ConditionalWriter.Result;
import core.client.ConditionalWriter.Status;
import core.client.impl.ConditionalWriterImpl;
import core.data.Condition;
import core.data.ConditionalMutation;

public class Transaction {
  
  private static final String SEP = Constants.SEP;
  private static final ColumnSet EMPTY_SET = new ColumnSet();
  
  private static final String DELETE = new String("special delete object");
  
  private long startTs;
  private Connector conn;
  private String table;
  
  private Map<String,Map<Column,String>> updates;
  private String triggerRow;
  private Column triggerColumn;
  private ColumnSet observedColumns;
  
  public Transaction(String table, Connector conn) {
    this(table, conn, null, null, EMPTY_SET);
  }
  
  public Transaction(String table, Connector conn, ColumnSet observedColumns) {
    this(table, conn, null, null, observedColumns);
  }
  
  public Transaction(String table, Connector conn, String triggerRow, Column tiggerColumn) {
    this(table, conn, triggerRow, tiggerColumn, EMPTY_SET);
  }
  
  public Transaction(String table, Connector conn, String triggerRow, Column tiggerColumn, ColumnSet observedColumns) {
    this.startTs = Oracle.getInstance().getTimestamp();
    this.conn = conn;
    this.table = table;
    this.updates = new HashMap<String,Map<Column,String>>();
    this.triggerRow = triggerRow;
    this.triggerColumn = tiggerColumn;
    this.observedColumns = observedColumns;
    
    if (triggerRow != null) {
      Map<Column,String> colUpdates = new HashMap<Column,String>();
      colUpdates.put(tiggerColumn, null);
      updates.put(triggerRow, colUpdates);
    }
  }
  
  public String get(String row, Column column) throws Exception {
    return get(row, Collections.singleton(column)).get(column);
  }
  
  public Map<Column,String> get(String row, Column start, Column end) throws Exception {
    Scanner scanner = conn.createScanner(table, new Authorizations());
    
    // TODO optimize with iterator
    // TODO only re-read columns that were locked instead of all columns
    // TODO cache row:col vals?
    
    mloop: while (true) {
      
      Range range = new Range(new Key(row, start.family, start.qualifier), true, new Key(row, end.family, end.qualifier), false);
      scanner.setRange(range);
      scanner.clearColumns();
      
      scanner.clearScanIterators();
      IteratorSetting iterConf = new IteratorSetting(10, SnapshotIterator.class);
      SnapshotIterator.setSnaptime(iterConf, startTs);
      scanner.addScanIterator(iterConf);
      
      Map<Column,String> ret = new HashMap<Column,String>();
      
      for (Entry<Key,Value> entry : scanner) {
        String cf = entry.getKey().getColumnFamilyData().toString();
        String cq = entry.getKey().getColumnQualifierData().toString();
        long colType = entry.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;
        
        if (colType == ColumnUtil.LOCK_PREFIX) {
          // TODO exponential back off and eventually do lock recovery
          UtilWaitThread.sleep(1000);
          continue mloop;
        } else if (colType == ColumnUtil.DATA_PREFIX) {
          ret.put(new Column(cf, cq), entry.getValue().toString());
        } else {
          throw new IllegalArgumentException();
        }
      }
      
      return ret;
    }
  }
  
  public Map<Column,String> get(String row, Set<Column> columns) throws Exception {
    Scanner scanner = conn.createScanner(table, new Authorizations());
    
    // TODO optimize with iterator
    // TODO only re-read columns that were locked instead of all columns
    // TODO cache row:col vals?
    
    mloop: while (true) {
      scanner.setRange(new Range(row));
      scanner.clearColumns();
      for (Column column : columns) {
        scanner.fetchColumn(new Text(column.family), new Text(column.qualifier));
        scanner.fetchColumn(new Text(column.family), new Text(column.qualifier));
        scanner.fetchColumn(new Text(column.family), new Text(column.qualifier));
      }
      
      scanner.clearScanIterators();
      IteratorSetting iterConf = new IteratorSetting(10, SnapshotIterator.class);
      SnapshotIterator.setSnaptime(iterConf, startTs);
      scanner.addScanIterator(iterConf);
      
      Map<Column,String> ret = new HashMap<Column,String>();
      
      for (Entry<Key,Value> entry : scanner) {
        String cf = entry.getKey().getColumnFamilyData().toString();
        String cq = entry.getKey().getColumnQualifierData().toString();
        long colType = entry.getKey().getTimestamp() & ColumnUtil.PREFIX_MASK;
        
        if (colType == ColumnUtil.LOCK_PREFIX) {
          // TODO exponential back off and eventually do lock recovery
          UtilWaitThread.sleep(1000);
          continue mloop;
        } else if (colType == ColumnUtil.DATA_PREFIX) {
          ret.put(new Column(cf, cq), entry.getValue().toString());
        } else {
          throw new IllegalArgumentException();
        }
      }
      
      return ret;
    }
  }
  
  public void set(String row, Column col, String value) {
    if (row == null || col == null || value == null) {
      throw new IllegalArgumentException();
    }
    
    Map<Column,String> colUpdates = updates.get(row);
    if (colUpdates == null) {
      colUpdates = new HashMap<Column,String>();
      updates.put(row, colUpdates);
    }
    
    colUpdates.put(col, value);
  }
  
  public void delete(String row, Column col) {
    set(row, col, DELETE);
  }
  
  private byte[] encode(long v) {
    byte ba[] = new byte[8];
    ba[0] = (byte) (v >>> 56);
    ba[1] = (byte) (v >>> 48);
    ba[2] = (byte) (v >>> 40);
    ba[3] = (byte) (v >>> 32);
    ba[4] = (byte) (v >>> 24);
    ba[5] = (byte) (v >>> 16);
    ba[6] = (byte) (v >>> 8);
    ba[7] = (byte) (v >>> 0);
    return ba;
  }
  
  private void releaseLock(boolean isTriggerRow, Column col, String val, long commitTs, Mutation m) {
    if (val != null) {
      m.put(new Text(col.family), new Text(col.qualifier), ColumnUtil.WRITE_PREFIX | commitTs, new Value(val == DELETE ? "D".getBytes() : encode(startTs)));
    } else {
      m.put(col.family, col.qualifier, ColumnUtil.DEL_LOCK_PREFIX | commitTs, "");
    }
    
    if (isTriggerRow && col.equals(triggerColumn)) {
      m.put(triggerColumn.family, triggerColumn.qualifier, ColumnUtil.ACK_PREFIX | startTs, "");
      m.putDelete(Constants.NOTIFY_CF, triggerColumn.family + SEP + triggerColumn.qualifier, startTs);
    }
    
    if (observedColumns.contains(col)) {
      m.put(Constants.NOTIFY_CF, col.family + SEP + col.qualifier, commitTs, "");
    }
  }
  
  private void prewrite(ConditionalMutation cm, Column col, String val, String primaryRow, Column primaryColumn, boolean isTriggerRow) {
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    if (isTriggerRow && col.equals(triggerColumn)) {
      PrewriteIterator.enableAckCheck(iterConf);
    }
    
    cm.addCondition(new Condition(col.family, col.qualifier).setIterators(iterConf));
    
    if (val != null && val != DELETE)
      cm.put(col.family, col.qualifier, ColumnUtil.DATA_PREFIX | startTs, val);
    cm.put(col.family, col.qualifier, ColumnUtil.LOCK_PREFIX | startTs, primaryRow + SEP + primaryColumn.family + SEP + primaryColumn.qualifier);
  }
  
  public boolean commit() throws Exception {
    ConditionalWriter cw = new ConditionalWriterImpl(table, conn, new Authorizations());
    
    // get a primary column
    String primaryRow = updates.keySet().iterator().next();
    Map<Column,String> colSet = updates.get(primaryRow);
    Column primaryColumn = colSet.keySet().iterator().next();
    String primaryValue = colSet.remove(primaryColumn);
    if (colSet.size() == 0)
      updates.remove(primaryRow);
    
    // try to lock primary column
    ConditionalMutation pcm = new ConditionalMutation(primaryRow);
    prewrite(pcm, primaryColumn, primaryValue, primaryRow, primaryColumn, primaryRow.equals(triggerRow));
    
    // TODO handle unknown
    if (cw.write(pcm).getStatus() != Status.ACCEPTED) {
      return false;
    }
    
    // try to lock other columns
    ArrayList<ConditionalMutation> mutations = new ArrayList<ConditionalMutation>();
    
    for (Entry<String,Map<Column,String>> rowUpdates : updates.entrySet()) {
      ConditionalMutation cm = new ConditionalMutation(rowUpdates.getKey());
      boolean isTriggerRow = rowUpdates.getKey().equals(triggerRow);
      
      for (Entry<Column,String> colUpdates : rowUpdates.getValue().entrySet()) {
        prewrite(cm, colUpdates.getKey(), colUpdates.getValue(), primaryRow, primaryColumn, isTriggerRow);
      }
      
      mutations.add(cm);
    }
    
    HashSet<String> acceptedRows = new HashSet<String>();
    int rejectedCount = 0;
    
    Iterator<Result> resultsIter = cw.write(mutations.iterator());
    while (resultsIter.hasNext()) {
      Result result = resultsIter.next();
      if (result.getStatus() == Status.ACCEPTED)
        acceptedRows.add(new String(result.getMutation().getRow()));
      else
        rejectedCount++;
    }
    
    if (rejectedCount == 0) {
      long commitTs = Oracle.getInstance().getTimestamp();
      
      // try to delete lock and add write for primary column
      ConditionalMutation delLockMutation = new ConditionalMutation(primaryRow);
      delLockMutation.addCondition(new Condition(primaryColumn.family, primaryColumn.qualifier).setTimestamp(ColumnUtil.LOCK_PREFIX | startTs).setValue(
          primaryRow + SEP + primaryColumn.family + SEP + primaryColumn.qualifier));
      releaseLock(primaryRow.equals(triggerRow), primaryColumn, primaryValue, commitTs, delLockMutation);
      
      if (cw.write(delLockMutation).getStatus() != Status.ACCEPTED) {
        // TODO rollback
        return false;
      }
      
      // delete locks and add writes for other columns
      BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
      for (Entry<String,Map<Column,String>> rowUpdates : updates.entrySet()) {
        Mutation m = new Mutation(rowUpdates.getKey());
        boolean isTriggerRow = rowUpdates.getKey().equals(triggerRow);
        for (Entry<Column,String> colUpdates : rowUpdates.getValue().entrySet()) {
          releaseLock(isTriggerRow, colUpdates.getKey(), colUpdates.getValue(), commitTs, m);
        }
        
        bw.addMutation(m);
      }
      
      bw.close();
      
      return true;
    } else {
      // roll back locks
      
      BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
      // TODO does order matter, should primary be deleted last?
      
      Mutation m = new Mutation(primaryRow);
      // TODO timestamp?
      m.put(primaryColumn.family, primaryColumn.qualifier, ColumnUtil.DEL_LOCK_PREFIX | startTs, "");
      bw.addMutation(m);
      
      for (String row : acceptedRows) {
        m = new Mutation(row);
        for (Column col : updates.get(row).keySet()) {
          m.put(col.family, col.qualifier, ColumnUtil.DEL_LOCK_PREFIX | startTs, "");
        }
        bw.addMutation(m);
      }
      
      bw.close();
      
      return false;
    }
  }
  
}
