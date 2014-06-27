package org.fluo.impl;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.commons.lang.mutable.MutableLong;

import org.fluo.api.Column;
import org.fluo.api.ColumnIterator;
import org.fluo.api.RowIterator;
import org.fluo.api.ScannerConfiguration;
import org.fluo.api.Transaction;
import org.fluo.api.exceptions.AlreadyAcknowledgedException;
import org.fluo.api.exceptions.AlreadySetException;
import org.fluo.api.exceptions.CommitException;
import org.fluo.impl.iterators.PrewriteIterator;


public class TransactionImpl implements Transaction {
  
  public static final byte[] EMPTY = new byte[0];
  public static final ByteSequence EMPTY_BS = new ArrayByteSequence(EMPTY);
  
  private static final ByteSequence DELETE = new ArrayByteSequence("special delete object");
  
  private long startTs;
  
  private Map<ByteSequence,Map<Column,ByteSequence>> updates;
  private Map<ByteSequence,Set<Column>> weakNotifications;
  Map<ByteSequence,Set<Column>> columnsRead = new HashMap<ByteSequence,Set<Column>>();
  private ByteSequence observer;
  private ByteSequence triggerRow;
  private Column triggerColumn;
  private ByteSequence weakRow;
  private Column weakColumn;
  private Set<Column> observedColumns;
  private boolean commitStarted = false;
  private Configuration config;
  private TxStats stats;
  
  public static byte[] toBytes(String s) {
    try {
      return s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  TransactionImpl(Configuration config, ByteSequence triggerRow, Column triggerColumn, Long startTs) throws Exception {
    this.config = config;
    this.observedColumns = config.getObservers().keySet();
    this.updates = new HashMap<ByteSequence,Map<Column,ByteSequence>>();
    this.weakNotifications = new HashMap<ByteSequence,Set<Column>>();
    
    if (triggerColumn != null && config.getWeakObservers().containsKey(triggerColumn)) {
      this.weakRow = triggerRow;
      this.weakColumn = triggerColumn;
    } else {
      this.triggerRow = triggerRow;
      this.triggerColumn = triggerColumn;
    }
    
    if (startTs == null)
      this.startTs = OracleClient.getInstance(config).getTimestamp();
    else {
      if (startTs < 0)
        throw new IllegalArgumentException();
      this.startTs = startTs;
    }
    
    if (triggerRow != null) {
      Map<Column,ByteSequence> colUpdates = new HashMap<Column,ByteSequence>();
      colUpdates.put(triggerColumn, null);
      updates.put(triggerRow, colUpdates);
      observer = new ArrayByteSequence("oid");
    }

    this.stats = new TxStats();
  }
  
  public TransactionImpl(Configuration config, ByteSequence triggerRow, Column tiggerColumn) throws Exception {
    this(config, triggerRow, tiggerColumn, null);
  }

  public TransactionImpl(Configuration config) throws Exception {
    this(config, null, null, null);
  }
  
  public TransactionImpl(Configuration config, long startTs) throws Exception {
    this(config, null, null, startTs);
  }

  @Override
  public ByteSequence get(ByteSequence row, Column column) throws Exception {
    // TODO cache? precache?
    return get(row, Collections.singleton(column)).get(column);
  }

  @Override
  public Map<Column,ByteSequence> get(ByteSequence row, Set<Column> columns) throws Exception {
    // TODO push visibility filtering to server side?

    ScannerConfiguration config = new ScannerConfiguration();
    config.setRange(new Range(ByteUtil.toText(row)));
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
    
    // only update columns read after successful read
    updateColumnsRead(row, columns);
    
    return ret;
  }
  
  @Override
  public Map<ByteSequence,Map<Column,ByteSequence>> get(Collection<ByteSequence> rows, Set<Column> columns) throws Exception {
    ParallelSnapshotScanner pss = new ParallelSnapshotScanner(rows, columns, config, startTs, stats);

    Map<ByteSequence,Map<Column,ByteSequence>> ret = pss.scan();

    for (Entry<ByteSequence,Map<Column,ByteSequence>> entry : ret.entrySet()) {
      updateColumnsRead(entry.getKey(), entry.getValue().keySet());
    }

    return ret;
  }

  private void updateColumnsRead(ByteSequence row, Set<Column> columns) {
    Set<Column> colsRead = columnsRead.get(row);
    if (colsRead == null) {
      colsRead = new HashSet<Column>();
      columnsRead.put(row, colsRead);
    }
    colsRead.addAll(columns);
  }

  // TODO add a get that uses the batch scanner

  @Override
  public RowIterator get(ScannerConfiguration config) throws Exception {
    if (commitStarted)
      throw new IllegalStateException("transaction committed");

    return new RowIteratorImpl(new SnapshotScanner(this.config, config, startTs, stats));
  }
  
  @Override
  public void set(ByteSequence row, Column col, ByteSequence value) {
    if (commitStarted)
      throw new IllegalStateException("transaction committed");

    ArgumentChecker.notNull(row, col, value);
    
    if (col.getFamily().equals(Constants.NOTIFY_CF)) {
      throw new IllegalArgumentException(Constants.NOTIFY_CF + " is a reserved family");
    }

    // TODO copy?

    Map<Column,ByteSequence> colUpdates = updates.get(row);
    if (colUpdates == null) {
      colUpdates = new HashMap<Column,ByteSequence>();
      updates.put(row, colUpdates);
    }
    
    if (colUpdates.get(col) != null) {
      throw new AlreadySetException("Value already set " + row + " " + col);
    }
    colUpdates.put(col, value);
  }
  

  @Override
  public void setWeakNotification(ByteSequence row, Column col) {
    if (commitStarted)
      throw new IllegalStateException("transaction committed");

    // TODO do not use ArgumentChecked
    // TODO anlyze code to see what non-public Accumulo APIs are used
    ArgumentChecker.notNull(row, col);

    if (!config.getWeakObservers().containsKey(col))
      throw new IllegalArgumentException("Column not configured for weak notifications " + col);

    Set<Column> columns = weakNotifications.get(row);
    if (columns == null) {
      columns = new HashSet<Column>();
      weakNotifications.put(row, columns);
    }

    columns.add(col);
  }

  @Override
  public void delete(ByteSequence row, Column col) {
    ArgumentChecker.notNull(row, col);
    set(row, col, DELETE);
  }
  
  private ConditionalMutation prewrite(ConditionalMutation cm, ByteSequence row, Column col, ByteSequence val, ByteSequence primaryRow, Column primaryColumn,
      boolean isTriggerRow) {
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    boolean isTrigger = isTriggerRow && col.equals(triggerColumn);
    if (isTrigger)
      PrewriteIterator.enableAckCheck(iterConf);
    
    Condition cond = new Condition(col.getFamily(), col.getQualifier()).setIterators(iterConf).setVisibility(col.getVisibility());
    
    if (cm == null)
      cm = new ConditionalMutation(row, cond);
    else
      cm.addCondition(cond);
    
    if (val != null && val != DELETE)
      cm.put(col.getFamily().toArray(), col.getQualifier().toArray(), col.getVisibility(), ColumnUtil.DATA_PREFIX | startTs, val.toArray());
    
    cm.put(col.getFamily().toArray(), col.getQualifier().toArray(), col.getVisibility(), ColumnUtil.LOCK_PREFIX | startTs,
        LockValue.encode(primaryRow, primaryColumn, val != null, val == DELETE, isTrigger ? observer : EMPTY_BS));
    
    return cm;
  }
  
  private ConditionalMutation prewrite(ByteSequence row, Column col, ByteSequence val, ByteSequence primaryRow, Column primaryColumn, boolean isTriggerRow) {
    return prewrite(null, row, col, val, primaryRow, primaryColumn, isTriggerRow);
  }

  private void prewrite(ConditionalMutation cm, Column col, ByteSequence val, ByteSequence primaryRow, Column primaryColumn, boolean isTriggerRow) {
    prewrite(cm, null, col, val, primaryRow, primaryColumn, isTriggerRow);
  }


  static class CommitData {
    ConditionalWriter cw;
    private ByteSequence prow;
    private Column pcol;
    private ByteSequence pval;

    private HashSet<ByteSequence> acceptedRows;
    private Map<ByteSequence,Set<Column>> rejected = new HashMap<ByteSequence,Set<Column>>();
    
    private void addPrimaryToRejected() {
      rejected = Collections.singletonMap(prow, Collections.singleton(pcol));
    }
    
    private void addToRejected(ByteSequence row, Set<Column> columns) {
      rejected = new HashMap<ByteSequence,Set<Column>>();
      
      Set<Column> ret = rejected.put(row, columns);
      if (ret != null)
        throw new IllegalStateException();
    }
    
    private Map<ByteSequence,Set<Column>> getRejected() {
      if (rejected == null)
        return Collections.emptyMap();
      
      return rejected;
    }

    public String toString() {
      return prow + " " + pcol + " " + pval + " " + rejected.size();
    }

  }

  boolean preCommit(CommitData cd) throws TableNotFoundException, AccumuloException, AccumuloSecurityException, AlreadyAcknowledgedException {
    if (triggerRow != null) {
      // always want to throw already ack exception if collision, so process trigger first
      return preCommit(cd, triggerRow, triggerColumn);
    } else {
      ByteSequence prow = updates.keySet().iterator().next();
      Map<Column,ByteSequence> colSet = updates.get(prow);
      Column pcol = colSet.keySet().iterator().next();
      return preCommit(cd, prow, pcol);
    }

  }
  
  boolean preCommit(CommitData cd, ByteSequence primRow, Column primCol) throws TableNotFoundException, AccumuloException, AccumuloSecurityException,
      AlreadyAcknowledgedException {
    if (commitStarted)
      throw new IllegalStateException();

    commitStarted = true;

    // get a primary column
    cd.prow = primRow;
    Map<Column,ByteSequence> colSet = updates.get(cd.prow);
    cd.pcol = primCol;
    cd.pval = colSet.remove(primCol);
    if (colSet.size() == 0)
      updates.remove(cd.prow);
    
    // try to lock primary column
    ConditionalMutation pcm = prewrite(cd.prow, cd.pcol, cd.pval, cd.prow, cd.pcol, cd.prow.equals(triggerRow));
    
    Status mutationStatus = cd.cw.write(pcm).getStatus();
    
    while (mutationStatus == Status.UNKNOWN) {
      
      MutableLong mcts = new MutableLong(-1);
      TxStatus txStatus = TxStatus.getTransactionStatus(config, cd.prow, cd.pcol, startTs, mcts, null);
      
      switch (txStatus) {
        case LOCKED:
          mutationStatus = Status.ACCEPTED;
          break;
        case ROLLED_BACK:
          mutationStatus = Status.REJECTED;
          break;
        case UNKNOWN:
          mutationStatus = cd.cw.write(pcm).getStatus();
          // TODO handle case were data other tx has lock
          break;
        case COMMITTED:
        default:
          throw new IllegalStateException("unexpected tx state " + txStatus + " " + cd.prow + " " + cd.pcol);
          
      }
    }
    
    if (mutationStatus != Status.ACCEPTED) {
      cd.addPrimaryToRejected();
      if (checkForAckCollision(pcm)) {
        throw new AlreadyAcknowledgedException();
      }
      return false;
    }
    
    // TODO if trigger is always primary row:col, then do not need checks elsewhere
    // try to lock other columns
    ArrayList<ConditionalMutation> mutations = new ArrayList<ConditionalMutation>();
    
    for (Entry<ByteSequence,Map<Column,ByteSequence>> rowUpdates : updates.entrySet()) {
      ConditionalMutation cm = null;
      boolean isTriggerRow = rowUpdates.getKey().equals(triggerRow);
      
      for (Entry<Column,ByteSequence> colUpdates : rowUpdates.getValue().entrySet()) {
        if (cm == null)
          cm = prewrite(rowUpdates.getKey(), colUpdates.getKey(), colUpdates.getValue(), cd.prow, cd.pcol, isTriggerRow);
        else
          prewrite(cm, colUpdates.getKey(), colUpdates.getValue(), cd.prow, cd.pcol, isTriggerRow);
      }
      
      mutations.add(cm);
    }
    
    cd.acceptedRows = new HashSet<ByteSequence>();
    
    boolean ackCollision = false;

    Iterator<Result> resultsIter = cd.cw.write(mutations.iterator());
    while (resultsIter.hasNext()) {
      Result result = resultsIter.next();
      // TODO handle unknown?
      ArrayByteSequence row = new ArrayByteSequence(result.getMutation().getRow());
      if (result.getStatus() == Status.ACCEPTED)
        cd.acceptedRows.add(row);
      else {
        // TODO if trigger is always primary row:col, then do not need checks elsewhere
        ackCollision |= checkForAckCollision(result.getMutation());
        cd.addToRejected(row, updates.get(row).keySet());
      }
    }
    
    if (cd.getRejected().size() > 0) {
      rollback(cd);
      
      if (ackCollision)
        throw new AlreadyAcknowledgedException();
      
      return false;
    }

    // set weak notifications after all locks are written, but before finishing commit. If weak notifications were set after the commit, then information
    // about weak notifications would need to be persisted in the lock phase. Setting here is safe because any observers that run as a result of the weak
    // notification will wait for the commit to finish. Setting here may cause an observer to run unessecarily in the case of rollback, but thats ok.
    // TODO look into setting weak notification as part of lock and commit phases to avoid this synchronous step
    writeWeakNotifications();

    return true;
  }

  private void writeWeakNotifications() {
    if (weakNotifications.size() > 0) {
      SharedBatchWriter sbw = config.getSharedResources().getBatchWriter();
      ArrayList<Mutation> mutations = new ArrayList<Mutation>();

      for (Entry<ByteSequence,Set<Column>> entry : weakNotifications.entrySet()) {
        Mutation m = new Mutation(entry.getKey().toArray());
        for (Column col : entry.getValue()) {
          m.put(Constants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(col), col.getVisibility(), startTs, TransactionImpl.EMPTY);
        }
        mutations.add(m);
      }
      sbw.writeMutations(mutations);
    }
  }

  /**
   * This function helps handle the following case
   * 
   * <OL>
   * <LI>TX1 locls r1 col1
   * <LI>TX1 fails before unlocking
   * <LI>TX2 attempts to write r1:col1 w/o reading it
   * </OL>
   * 
   * In this case TX2 would not roll back TX1, because it never read the column. This function attempts to handle this case if TX2 fails. Only doing this in
   * case of failures is cheaper than trying to always read unread columns.
   * 
   * @param cd
   */
  private void readUnread(CommitData cd) throws Exception {
    // TODO need to keep track of ranges read (not ranges passed in, but actual data read... user may not iterate over entire range
    Map<ByteSequence,Set<Column>> columnsToRead = new HashMap<ByteSequence,Set<Column>>();
    
    for (Entry<ByteSequence,Set<Column>> entry : cd.getRejected().entrySet()) {
      Set<Column> rowColsRead = columnsRead.get(entry.getKey());
      if (rowColsRead == null) {
        columnsToRead.put(entry.getKey(), entry.getValue());
      } else {
        HashSet<Column> colsToRead = new HashSet<Column>(entry.getValue());
        colsToRead.removeAll(rowColsRead);
        if (colsToRead.size() > 0) {
          columnsToRead.put(entry.getKey(), colsToRead);
        }
      }
    }
    
    boolean cs = commitStarted;
    try {
      // TODO setting commitStarted false here is a bit of a hack... reuse code w/o doing this
      commitStarted = false;
      for (Entry<ByteSequence,Set<Column>> entry : columnsToRead.entrySet()) {
        get(entry.getKey(), entry.getValue());
      }
    } finally {
      commitStarted = cs;
    }
  }

  private boolean checkForAckCollision(ConditionalMutation cm) {
    ArrayByteSequence row = new ArrayByteSequence(cm.getRow());

    if (row.equals(triggerRow)) {
      List<ColumnUpdate> updates = cm.getUpdates();
      
      for (ColumnUpdate cu : updates) {
        // TODO avoid create col vis object
        Column col = new Column(new ArrayByteSequence(cu.getColumnFamily()), new ArrayByteSequence(cu.getColumnQualifier()))
            .setVisibility(new ColumnVisibility(cu.getColumnVisibility()));
        if (triggerColumn.equals(col)) {
          
          // TODO this check will not detect ack when tx overlaps with another tx... it will instead the the lock release.. this may be ok, the worker will
          // retry the tx and then see the already ack exception

          IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
          PrewriteIterator.setSnaptime(iterConf, startTs);
          PrewriteIterator.enableAckCheck(iterConf);
          Key key = ColumnUtil.checkColumn(config, iterConf, row, col).getKey();
          // TODO could key be null?
          long colType = key.getTimestamp() & ColumnUtil.PREFIX_MASK;
          
          if (colType == ColumnUtil.ACK_PREFIX) {
            return true;
          }
        }
      }
    }
    
    return false;
  }

  boolean commitPrimaryColumn(CommitData cd, long commitTs) throws AccumuloException, AccumuloSecurityException {
    // try to delete lock and add write for primary column
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    boolean isTrigger = cd.prow.equals(triggerRow) && cd.pcol.equals(triggerColumn);
    Condition lockCheck = new Condition(cd.pcol.getFamily(), cd.pcol.getQualifier()).setIterators(iterConf).setVisibility(cd.pcol.getVisibility())
        .setValue(LockValue.encode(cd.prow, cd.pcol, cd.pval != null, cd.pval == DELETE, isTrigger ? observer : EMPTY_BS));
    ConditionalMutation delLockMutation = new ConditionalMutation(cd.prow, lockCheck);
    ColumnUtil.commitColumn(isTrigger, true, cd.pcol, cd.pval != null, cd.pval == DELETE, startTs, commitTs, observedColumns, delLockMutation);
    
    Status mutationStatus = cd.cw.write(delLockMutation).getStatus();
    
    while (mutationStatus == Status.UNKNOWN) {
      
      MutableLong mcts = new MutableLong(-1);
      TxStatus txStatus = TxStatus.getTransactionStatus(config, cd.prow, cd.pcol, startTs, mcts, null);
      
      switch (txStatus) {
        case COMMITTED:
          if (mcts.longValue() != commitTs)
            throw new IllegalStateException(cd.prow + " " + cd.pcol + " " + mcts.longValue() + "!=" + commitTs);
          mutationStatus = Status.ACCEPTED;
          break;
        case LOCKED:
          mutationStatus = cd.cw.write(delLockMutation).getStatus();
          break;
        default:
          mutationStatus = Status.REJECTED;
      }
    }

    if (mutationStatus != Status.ACCEPTED) {
      return false;
    }
    
    return true;
  }
  
  private void rollback(CommitData cd) throws TableNotFoundException, MutationsRejectedException {
    // roll back locks
    
    // TODO let rollback be done lazily? this makes GC more difficult
    
    Mutation m;

    ArrayList<Mutation> mutations = new ArrayList<Mutation>(cd.acceptedRows.size());
    for (ByteSequence row : cd.acceptedRows) {
      m = new Mutation(row.toArray());
      for (Column col : updates.get(row).keySet()) {
        m.put(col.getFamily().toArray(), col.getQualifier().toArray(), col.getVisibility(), ColumnUtil.DEL_LOCK_PREFIX | startTs,
            DelLockValue.encode(startTs, false, true));
      }
      mutations.add(m);
    }
    
    config.getSharedResources().getBatchWriter().writeMutations(mutations);
    
    // mark transaction as complete for garbage collection purposes
    m = new Mutation(cd.prow.toArray());
    // TODO timestamp?
    // TODO writing the primary column with a batch writer is iffy
    m.put(cd.pcol.getFamily().toArray(), cd.pcol.getQualifier().toArray(), cd.pcol.getVisibility(), ColumnUtil.DEL_LOCK_PREFIX | startTs,
        DelLockValue.encode(startTs, false, true));
    m.put(cd.pcol.getFamily().toArray(), cd.pcol.getQualifier().toArray(), cd.pcol.getVisibility(), ColumnUtil.TX_DONE_PREFIX | startTs, EMPTY);
    config.getSharedResources().getBatchWriter().writeMutation(m);
  }
  
  boolean finishCommit(CommitData cd, long commitTs) throws TableNotFoundException, MutationsRejectedException {
    // delete locks and add writes for other columns
    ArrayList<Mutation> mutations = new ArrayList<Mutation>(updates.size() + 1);
    for (Entry<ByteSequence,Map<Column,ByteSequence>> rowUpdates : updates.entrySet()) {
      Mutation m = new Mutation(rowUpdates.getKey().toArray());
      boolean isTriggerRow = rowUpdates.getKey().equals(triggerRow);
      for (Entry<Column,ByteSequence> colUpdates : rowUpdates.getValue().entrySet()) {
        ColumnUtil.commitColumn(isTriggerRow && colUpdates.getKey().equals(triggerColumn), false, colUpdates.getKey(), colUpdates.getValue() != null,
            colUpdates.getValue() == DELETE, startTs, commitTs, observedColumns, m);
      }
      
      mutations.add(m);
    }
    
    if (weakRow != null) {
      Mutation m = new Mutation(weakRow.toArray());
      m.putDelete(Constants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(weakColumn), weakColumn.getVisibility(), commitTs);
      mutations.add(m);
    }

    config.getSharedResources().getBatchWriter().writeMutations(mutations);
    
    // mark transaction as complete for garbage collection purposes
    Mutation m = new Mutation(cd.prow.toArray());
    m.put(cd.pcol.getFamily().toArray(), cd.pcol.getQualifier().toArray(), cd.pcol.getVisibility(), ColumnUtil.TX_DONE_PREFIX | commitTs, EMPTY);
    config.getSharedResources().getBatchWriter().writeMutationAsync(m);
    
    return true;
  }

  CommitData createCommitData() {
    CommitData cd = new CommitData();
    cd.cw = config.getSharedResources().getConditionalWriter();
    return cd;
  }

  public void commit() throws CommitException {
    // TODO synchronize or detect concurrent use
    // TODO prevent multiple calls
    
    if (updates.size() == 0) {
      deleteWeakRow();
      return;
    }

    for (Map<Column,ByteSequence> cols : updates.values())
      stats.incrementEntriesSet(cols.size());

    CommitData cd = createCommitData();
    
    try {
      if (!preCommit(cd)) {
        readUnread(cd);
        throw new CommitException();
      }
      
      long commitTs = OracleClient.getInstance(config).getTimestamp();
      if (commitPrimaryColumn(cd, commitTs)) {
        finishCommit(cd, commitTs);
      } else {
        // TODO write TX_DONE
        throw new CommitException();
      }

    } catch (CommitException e) {
      throw e;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      stats.setFinishTime(System.currentTimeMillis());
      for (Set<Column> cols : cd.getRejected().values()) {
        stats.incrementCollisions(cols.size());
      }
    }
  }

  void deleteWeakRow() {
    if (weakRow != null) {
      long commitTs;
      try {
        commitTs = OracleClient.getInstance(config).getTimestamp();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      Mutation m = new Mutation(weakRow.toArray());
      m.putDelete(Constants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(weakColumn), weakColumn.getVisibility(), commitTs);
      config.getSharedResources().getBatchWriter().writeMutation(m);
    }
  }

  TxStats getStats() {
    return stats;
  }

  long getStartTs() {
    return startTs;
  }
}
