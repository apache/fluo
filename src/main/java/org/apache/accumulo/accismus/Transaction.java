package org.apache.accumulo.accismus;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.accismus.impl.ByteUtil;
import org.apache.accumulo.accismus.impl.ColumnUtil;
import org.apache.accumulo.accismus.impl.SnapshotScanner;
import org.apache.accumulo.accismus.iterators.PrewriteIterator;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;

import core.client.ConditionalWriter;
import core.client.ConditionalWriter.Result;
import core.client.ConditionalWriter.Status;
import core.client.impl.ConditionalWriterImpl;
import core.data.Condition;
import core.data.ConditionalMutation;

public class Transaction {
  
  private static final ColumnSet EMPTY_SET = new ColumnSet();
  private static final byte[] EMPTY = new byte[0];
  
  private static final ByteSequence DELETE = new ArrayByteSequence("special delete object");
  
  private long startTs;
  private Connector conn;
  private String table;
  
  private Map<ByteSequence,Map<Column,ByteSequence>> updates;
  private ByteSequence triggerRow;
  private Column triggerColumn;
  private ColumnSet observedColumns;
  
  private static byte[] toBytes(String s) {
    try {
      return s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private static Text toText(ByteSequence bs) {
    if (bs.isBackedByArray()) {
      Text t = new Text(EMPTY);
      t.set(bs.getBackingArray(), bs.offset(), bs.length());
      return t;
    } else {
      return new Text(bs.toArray());
    }
  }

  private byte[] concat(Column c) {
    return ByteUtil.concat(c.getFamily(), c.getQualifier());
  }
  
  private byte[] concat(ByteSequence row, Column c) {
    return ByteUtil.concat(row, c.getFamily(), c.getQualifier(), new ArrayByteSequence(c.getVisibility().getExpression()));
  }

  public Transaction(String table, Connector conn) {
    this(table, conn, null, null, EMPTY_SET);
  }
  
  public Transaction(String table, Connector conn, ColumnSet observedColumns) {
    this(table, conn, null, null, observedColumns);
  }
  
  public Transaction(String table, Connector conn, ByteSequence triggerRow, Column tiggerColumn) {
    this(table, conn, triggerRow, tiggerColumn, EMPTY_SET);
  }
  
  public Transaction(String table, Connector conn, ByteSequence triggerRow, Column tiggerColumn, ColumnSet observedColumns) {
    this.startTs = Oracle.getInstance().getTimestamp();
    this.conn = conn;
    this.table = table;
    this.updates = new HashMap<ByteSequence,Map<Column,ByteSequence>>();
    this.triggerRow = triggerRow;
    this.triggerColumn = tiggerColumn;
    this.observedColumns = observedColumns;
    
    if (triggerRow != null) {
      Map<Column,ByteSequence> colUpdates = new HashMap<Column,ByteSequence>();
      colUpdates.put(tiggerColumn, null);
      updates.put(triggerRow, colUpdates);
    }
  }
  
  public ByteSequence get(String row, Column column) throws Exception {
    return get(new ArrayByteSequence(toBytes(row)), column);
  }
  
  public ByteSequence get(byte[] row, Column column) throws Exception {
    return get(new ArrayByteSequence(row), column);
  }

  public ByteSequence get(ByteSequence row, Column column) throws Exception {
    // TODO cache? precache?
    return get(row, Collections.singleton(column)).get(column);
  }
  
  public Map<Column,ByteSequence> get(String row, Set<Column> columns) throws Exception {
    return get(new ArrayByteSequence(toBytes(row)), columns);
  }
  
  public Map<Column,ByteSequence> get(byte[] row, Set<Column> columns) throws Exception {
    return get(new ArrayByteSequence(row), columns);
  }

  public Map<Column,ByteSequence> get(ByteSequence row, Set<Column> columns) throws Exception {
    // TODO push visibility filtering to server side?

    ScannerConfiguration config = new ScannerConfiguration();
    config.setRange(new Range(toText(row)));
    for (Column column : columns) {
      config.fetchColumn(column.getFamily(), column.getQualifier());
    }
    
    RowIterator iter = get(config);
    
    Map<Column,ByteSequence> ret = new HashMap<Column,ByteSequence>();

    while (iter.hasNext()) {
      Entry<ByteSequence,ColumnIterator> entry = iter.next();
      ColumnIterator citer = entry.getValue();
      while (citer.hasNext()) {
        Entry<Column,ByteSequence> centry = citer.next();
        if (columns.contains(centry.getKey())) {
          ret.put(centry.getKey(), centry.getValue());
        }
      }
    }
    
    return ret;
  }

  public RowIterator get(ScannerConfiguration config) throws Exception {
    return new RowIterator(new SnapshotScanner(conn, table, config, startTs));
  }
  
  public void set(String row, Column col, String value) {
    ArgumentChecker.notNull(row, col, value);
    set(new ArrayByteSequence(toBytes(row)), col, new ArrayByteSequence(toBytes(value)));
  }
  
  public void set(byte[] row, Column col, byte[] value) {
    ArgumentChecker.notNull(row, col, value);
    set(new ArrayByteSequence(row), col, new ArrayByteSequence(value));
  }
  
  public void set(ByteSequence row, Column col, ByteSequence value) {
    ArgumentChecker.notNull(row, col, value);
    
    // TODO copy?

    Map<Column,ByteSequence> colUpdates = updates.get(row);
    if (colUpdates == null) {
      colUpdates = new HashMap<Column,ByteSequence>();
      updates.put(row, colUpdates);
    }
    
    colUpdates.put(col, value);
  }
  
  public void delete(String row, Column col) {
    ArgumentChecker.notNull(row, col);
    set(new ArrayByteSequence(toBytes(row)), col, DELETE);
  }
  
  public void delete(byte[] row, Column col) {
    ArgumentChecker.notNull(row, col);
    set(new ArrayByteSequence(row), col, DELETE);
  }
  
  public void delete(ByteSequence row, Column col) {
    ArgumentChecker.notNull(row, col);
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

  private void releaseLock(boolean isTriggerRow, Column col, ByteSequence val, long commitTs, Mutation m) {
    if (val != null) {
      m.put(toText(col.getFamily()), toText(col.getQualifier()), col.getVisibility(), ColumnUtil.WRITE_PREFIX | commitTs, new Value(
          val == DELETE ? toBytes("D") : encode(startTs)));
    } else {
      m.put(col.getFamily().toArray(), col.getQualifier().toArray(), col.getVisibility(), ColumnUtil.DEL_LOCK_PREFIX | commitTs, EMPTY);
    }
    
    if (isTriggerRow && col.equals(triggerColumn)) {
      m.put(triggerColumn.getFamily().toArray(), triggerColumn.getQualifier().toArray(), triggerColumn.getVisibility(), ColumnUtil.ACK_PREFIX | startTs, EMPTY);
      m.putDelete(toBytes(Constants.NOTIFY_CF), concat(triggerColumn), triggerColumn.getVisibility(), startTs);
    }
    
    if (observedColumns.contains(col)) {
      m.put(toBytes(Constants.NOTIFY_CF), concat(col), col.getVisibility(), commitTs, EMPTY);
    }
  }
  
  private void prewrite(ConditionalMutation cm, Column col, ByteSequence val, ByteSequence primaryRow, Column primaryColumn, boolean isTriggerRow) {
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    if (isTriggerRow && col.equals(triggerColumn)) {
      PrewriteIterator.enableAckCheck(iterConf);
    }
    
    cm.addCondition(new Condition(col.getFamily(), col.getQualifier()).setIterators(iterConf).setVisibility(col.getVisibility()));
    
    if (val != null && val != DELETE)
      cm.put(col.getFamily().toArray(), col.getQualifier().toArray(), col.getVisibility(), ColumnUtil.DATA_PREFIX | startTs, val.toArray());

    cm.put(col.getFamily().toArray(), col.getQualifier().toArray(), col.getVisibility(), ColumnUtil.LOCK_PREFIX | startTs, concat(primaryRow, primaryColumn));
  }
  
  public boolean commit() throws Exception {
    ConditionalWriter cw = new ConditionalWriterImpl(table, conn, new Authorizations());
    
    // get a primary column
    ByteSequence primaryRow = updates.keySet().iterator().next();
    Map<Column,ByteSequence> colSet = updates.get(primaryRow);
    Column primaryColumn = colSet.keySet().iterator().next();
    ByteSequence primaryValue = colSet.remove(primaryColumn);
    if (colSet.size() == 0)
      updates.remove(primaryRow);
    
    // try to lock primary column
    ConditionalMutation pcm = new ConditionalMutation(primaryRow.toArray());
    prewrite(pcm, primaryColumn, primaryValue, primaryRow, primaryColumn, primaryRow.equals(triggerRow));
    
    // TODO handle unknown
    if (cw.write(pcm).getStatus() != Status.ACCEPTED) {
      return false;
    }
    
    // try to lock other columns
    ArrayList<ConditionalMutation> mutations = new ArrayList<ConditionalMutation>();
    
    for (Entry<ByteSequence,Map<Column,ByteSequence>> rowUpdates : updates.entrySet()) {
      ConditionalMutation cm = new ConditionalMutation(rowUpdates.getKey().toArray());
      boolean isTriggerRow = rowUpdates.getKey().equals(triggerRow);
      
      for (Entry<Column,ByteSequence> colUpdates : rowUpdates.getValue().entrySet()) {
        prewrite(cm, colUpdates.getKey(), colUpdates.getValue(), primaryRow, primaryColumn, isTriggerRow);
      }
      
      mutations.add(cm);
    }
    
    HashSet<ByteSequence> acceptedRows = new HashSet<ByteSequence>();
    int rejectedCount = 0;
    
    Iterator<Result> resultsIter = cw.write(mutations.iterator());
    while (resultsIter.hasNext()) {
      Result result = resultsIter.next();
      if (result.getStatus() == Status.ACCEPTED)
        acceptedRows.add(new ArrayByteSequence(result.getMutation().getRow()));
      else
        rejectedCount++;
    }
    
    if (rejectedCount == 0) {
      long commitTs = Oracle.getInstance().getTimestamp();
      
      // try to delete lock and add write for primary column
      ConditionalMutation delLockMutation = new ConditionalMutation(primaryRow.toArray());
      IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
      PrewriteIterator.setSnaptime(iterConf, startTs);
      delLockMutation.addCondition(new Condition(primaryColumn.getFamily(), primaryColumn.getQualifier()).setTimestamp(ColumnUtil.LOCK_PREFIX | startTs)
          .setIterators(iterConf).setVisibility(primaryColumn.getVisibility()).setValue(concat(primaryRow, primaryColumn)));
      releaseLock(primaryRow.equals(triggerRow), primaryColumn, primaryValue, commitTs, delLockMutation);
      
      if (cw.write(delLockMutation).getStatus() != Status.ACCEPTED) {
        // TODO rollback
        return false;
      }
      
      // delete locks and add writes for other columns
      BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
      for (Entry<ByteSequence,Map<Column,ByteSequence>> rowUpdates : updates.entrySet()) {
        Mutation m = new Mutation(rowUpdates.getKey().toArray());
        boolean isTriggerRow = rowUpdates.getKey().equals(triggerRow);
        for (Entry<Column,ByteSequence> colUpdates : rowUpdates.getValue().entrySet()) {
          releaseLock(isTriggerRow, colUpdates.getKey(), colUpdates.getValue(), commitTs, m);
        }
        
        bw.addMutation(m);
      }
      
      bw.close();
      
      return true;
    } else {
      // roll back locks
      
      // TODO let rollback be done lazily?

      BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
      // TODO does order matter, should primary be deleted last?
      
      Mutation m = new Mutation(primaryRow.toArray());
      // TODO timestamp?
      m.put(primaryColumn.getFamily().toArray(), primaryColumn.getQualifier().toArray(), primaryColumn.getVisibility(), ColumnUtil.DEL_LOCK_PREFIX | startTs,
          toBytes(""));
      bw.addMutation(m);
      
      for (ByteSequence row : acceptedRows) {
        m = new Mutation(row.toArray());
        for (Column col : updates.get(row).keySet()) {
          m.put(col.getFamily().toArray(), col.getQualifier().toArray(), col.getVisibility(), ColumnUtil.DEL_LOCK_PREFIX | startTs, toBytes(""));
        }
        bw.addMutation(m);
      }
      
      bw.close();
      
      return false;
    }
  }
}
