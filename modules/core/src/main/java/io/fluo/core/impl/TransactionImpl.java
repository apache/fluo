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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.fluo.accumulo.iterators.PrewriteIterator;
import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.values.DelLockValue;
import io.fluo.accumulo.values.LockValue;
import io.fluo.api.client.Snapshot;
import io.fluo.api.client.Transaction;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.exceptions.AlreadySetException;
import io.fluo.api.exceptions.CommitException;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.core.exceptions.AlreadyAcknowledgedException;
import io.fluo.core.oracle.OracleClient;
import io.fluo.core.util.ColumnUtil;
import io.fluo.core.util.ConditionalFlutation;
import io.fluo.core.util.FluoCondition;
import io.fluo.core.util.Flutation;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Transaction implementation
 */
public class TransactionImpl implements Transaction, Snapshot {
  
  public static final byte[] EMPTY = new byte[0];
  public static final Bytes EMPTY_BS = Bytes.wrap(EMPTY);
  private static final Bytes DELETE = Bytes.wrap("special delete object");
  private static enum TxStatus { OPEN, COMMIT_STARTED, COMMITTED, CLOSED };
  
  private final long startTs;
  private final Map<Bytes,Map<Column,Bytes>> updates = new HashMap<>();;
  private final Map<Bytes,Set<Column>> weakNotifications = new HashMap<>();
  private final Set<Column> observedColumns;
  private final Environment env;
  final Map<Bytes,Set<Column>> columnsRead = new HashMap<>();
  private final TxStats stats = new TxStats();
  private Bytes triggerRow;
  private Column triggerColumn;
  private Bytes weakRow;
  private Column weakColumn;
  private TransactorNode tnode = null;
  private TxStatus status = TxStatus.OPEN;

  private ColumnVisibility gv(Column colvis) {
    return env.getSharedResources().getVisCache().getCV(colvis);
  }
 
  public TransactionImpl(Environment env, Bytes triggerRow, Column triggerColumn, long startTs) {
    Preconditions.checkNotNull(env, "environment cannot be null");
    Preconditions.checkArgument(startTs >= 0, "startTs cannot be negative");
    this.env = env;
    this.startTs = startTs;
    this.observedColumns = env.getObservers().keySet();
    
    if (triggerColumn != null && env.getWeakObservers().containsKey(triggerColumn)) {
      this.weakRow = triggerRow;
      this.weakColumn = triggerColumn;
    } else {
      this.triggerRow = triggerRow;
      this.triggerColumn = triggerColumn;
    }
    
    if (triggerRow != null) {
      Map<Column,Bytes> colUpdates = new HashMap<>();
      colUpdates.put(triggerColumn, null);
      updates.put(triggerRow, colUpdates);
    }
  }
  
  public TransactionImpl(Environment env, Bytes triggerRow, Column tiggerColumn) {
    this(env, triggerRow, tiggerColumn, allocateTimestamp(env));
  }

  public TransactionImpl(Environment env) {
    this(env, null, null, allocateTimestamp(env));
  }
  
  public TransactionImpl(Environment env, long startTs) {
    this(env, null, null, startTs);
  }
    
  private static Long allocateTimestamp(Environment env) {
    return env.getSharedResources().getTimestampTracker().allocateTimestamp();
  }

  @Override
  public Bytes get(Bytes row, Column column) throws Exception {
    checkIfOpen();
    // TODO cache? precache?
    return get(row, Collections.singleton(column)).get(column);
  }

  @Override
  public Map<Column,Bytes> get(Bytes row, Set<Column> columns) throws Exception {
    checkIfOpen();
    return getImpl(row, columns);
  }
  
  private Map<Column,Bytes> getImpl(Bytes row, Set<Column> columns) throws Exception {
    
    // TODO push visibility filtering to server side?

    env.getSharedResources().getVisCache().validate(columns);

    ScannerConfiguration config = new ScannerConfiguration();
    config.setSpan(Span.exact(row));
    for (Column column : columns) {
      config.fetchColumn(column.getFamily(), column.getQualifier());
    }
    
    RowIterator iter = getImpl(config);
    
    Map<Column,Bytes> ret = new HashMap<>();

    while (iter.hasNext()) {
      Entry<Bytes,ColumnIterator> entry = iter.next();
      ColumnIterator citer = entry.getValue();
      while (citer.hasNext()) {
        Entry<Column,Bytes> centry = citer.next();
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
  public Map<Bytes,Map<Column,Bytes>> get(Collection<Bytes> rows, Set<Column> columns) throws Exception {
    checkIfOpen();

    env.getSharedResources().getVisCache().validate(columns);

    ParallelSnapshotScanner pss = new ParallelSnapshotScanner(rows, columns, env, startTs, stats);

    Map<Bytes,Map<Column,Bytes>> ret = pss.scan();

    for (Entry<Bytes,Map<Column,Bytes>> entry : ret.entrySet()) {
      updateColumnsRead(entry.getKey(), entry.getValue().keySet());
    }

    return ret;
  }

  private void updateColumnsRead(Bytes row, Set<Column> columns) {
    Set<Column> colsRead = columnsRead.get(row);
    if (colsRead == null) {
      colsRead = new HashSet<>();
      columnsRead.put(row, colsRead);
    }
    colsRead.addAll(columns);
  }

  // TODO add a get that uses the batch scanner

  @Override
  public RowIterator get(ScannerConfiguration config) throws Exception {
    checkIfOpen();
    return getImpl(config);
  }
  
  private RowIterator getImpl(ScannerConfiguration config) throws Exception {
    return new RowIteratorImpl(new SnapshotScanner(this.env, config, startTs, stats));
  }
    
  @Override
  public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
    checkIfOpen();
    Preconditions.checkNotNull(row);
    Preconditions.checkNotNull(col);
    Preconditions.checkNotNull(value);
    
    if (col.getFamily().equals(ColumnConstants.NOTIFY_CF)) {
      throw new IllegalArgumentException(ColumnConstants.NOTIFY_CF + " is a reserved family");
    }

    env.getSharedResources().getVisCache().validate(col);

    // TODO copy?

    Map<Column,Bytes> colUpdates = updates.get(row);
    if (colUpdates == null) {
      colUpdates = new HashMap<>();
      updates.put(row, colUpdates);
    }
    
    if (colUpdates.get(col) != null) {
      throw new AlreadySetException("Value already set " + row + " " + col);
    }
    colUpdates.put(col, value);
  }
  

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    checkIfOpen();
    Preconditions.checkNotNull(row);
    Preconditions.checkNotNull(col);

    if (!env.getWeakObservers().containsKey(col))
      throw new IllegalArgumentException("Column not configured for weak notifications " + col);

    env.getSharedResources().getVisCache().validate(col);

    Set<Column> columns = weakNotifications.get(row);
    if (columns == null) {
      columns = new HashSet<>();
      weakNotifications.put(row, columns);
    }

    columns.add(col);
  }

  @Override
  public void delete(Bytes row, Column col) throws AlreadySetException {
    checkIfOpen();
    Preconditions.checkNotNull(row);
    Preconditions.checkNotNull(col);
    set(row, col, DELETE);
  }
  
  private ConditionalFlutation prewrite(ConditionalFlutation cm, Bytes row, Column col, Bytes val, Bytes primaryRow, Column primaryColumn,
      boolean isTriggerRow) {
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    boolean isTrigger = isTriggerRow && col.equals(triggerColumn);
    if (isTrigger)
      PrewriteIterator.enableAckCheck(iterConf);
    
    Condition cond = new FluoCondition(env, col).setIterators(iterConf);
    
    if (cm == null)
      cm = new ConditionalFlutation(env, row, cond);
    else
      cm.addCondition(cond);
    
    if (val != null && val != DELETE)
      cm.put(col, ColumnConstants.DATA_PREFIX | startTs, val.toArray());

    cm.put(col, ColumnConstants.LOCK_PREFIX | startTs, LockValue.encode(primaryRow, primaryColumn, val != null, val == DELETE, isTriggerRow, getTransactorID()));
    
    return cm;
  }
  
  private ConditionalFlutation prewrite(Bytes row, Column col, Bytes val, Bytes primaryRow, Column primaryColumn, boolean isTriggerRow) {
    return prewrite(null, row, col, val, primaryRow, primaryColumn, isTriggerRow);
  }

  private void prewrite(ConditionalFlutation cm, Column col, Bytes val, Bytes primaryRow, Column primaryColumn, boolean isTriggerRow) {
    prewrite(cm, null, col, val, primaryRow, primaryColumn, isTriggerRow);
  }


  public static class CommitData {
    ConditionalWriter cw;
    private Bytes prow;
    private Column pcol;
    private Bytes pval;

    private HashSet<Bytes> acceptedRows;
    private Map<Bytes,Set<Column>> rejected = new HashMap<>();
    
    private void addPrimaryToRejected() {
      rejected = Collections.singletonMap(prow, Collections.singleton(pcol));
    }
    
    private void addToRejected(Bytes row, Set<Column> columns) {
      rejected = new HashMap<>();
      
      Set<Column> ret = rejected.put(row, columns);
      if (ret != null)
        throw new IllegalStateException();
    }
    
    private Map<Bytes,Set<Column>> getRejected() {
      if (rejected == null)
        return Collections.emptyMap();
      
      return rejected;
    }

    @Override
    public String toString() {
      return prow + " " + pcol + " " + pval + " " + rejected.size();
    }
  }

  public boolean preCommit(CommitData cd) throws TableNotFoundException, AccumuloException, AccumuloSecurityException, AlreadyAcknowledgedException {
    if (triggerRow != null) {
      // always want to throw already ack exception if collision, so process trigger first
      return preCommit(cd, triggerRow, triggerColumn);
    } else {
      Bytes prow = updates.keySet().iterator().next();
      Map<Column,Bytes> colSet = updates.get(prow);
      Column pcol = colSet.keySet().iterator().next();
      return preCommit(cd, prow, pcol);
    }

  }
  
  public boolean preCommit(CommitData cd, Bytes primRow, Column primCol) throws TableNotFoundException, AccumuloException, AccumuloSecurityException,
      AlreadyAcknowledgedException {
    
    checkIfOpen();
    status = TxStatus.COMMIT_STARTED;

    // get a primary column
    cd.prow = primRow;
    Map<Column,Bytes> colSet = updates.get(cd.prow);
    cd.pcol = primCol;
    cd.pval = colSet.remove(primCol);
    if (colSet.size() == 0)
      updates.remove(cd.prow);
    
    // try to lock primary column
    ConditionalMutation pcm = prewrite(cd.prow, cd.pcol, cd.pval, cd.prow, cd.pcol, cd.prow.equals(triggerRow));
    
    Status mutationStatus = cd.cw.write(pcm).getStatus();
    
    while (mutationStatus == Status.UNKNOWN) {
      
      TxInfo txInfo = TxInfo.getTransactionInfo(env, cd.prow, cd.pcol, startTs);
      
      switch (txInfo.status) {
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
          throw new IllegalStateException("unexpected tx state " + txInfo.status + " " + cd.prow + " " + cd.pcol);
          
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
    ArrayList<ConditionalMutation> mutations = new ArrayList<>();
    
    for (Entry<Bytes,Map<Column,Bytes>> rowUpdates : updates.entrySet()) {
      ConditionalFlutation cm = null;
      boolean isTriggerRow = rowUpdates.getKey().equals(triggerRow);
      
      for (Entry<Column,Bytes> colUpdates : rowUpdates.getValue().entrySet()) {
        if (cm == null)
          cm = prewrite(rowUpdates.getKey(), colUpdates.getKey(), colUpdates.getValue(), cd.prow, cd.pcol, isTriggerRow);
        else
          prewrite(cm, colUpdates.getKey(), colUpdates.getValue(), cd.prow, cd.pcol, isTriggerRow);
      }
      
      mutations.add(cm);
    }
    
    cd.acceptedRows = new HashSet<>();
    
    boolean ackCollision = false;

    Iterator<Result> resultsIter = cd.cw.write(mutations.iterator());
    while (resultsIter.hasNext()) {
      Result result = resultsIter.next();
      // TODO handle unknown?
      Bytes row = Bytes.wrap(result.getMutation().getRow());
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
      SharedBatchWriter sbw = env.getSharedResources().getBatchWriter();
      ArrayList<Mutation> mutations = new ArrayList<>();

      for (Entry<Bytes,Set<Column>> entry : weakNotifications.entrySet()) {
        Flutation m = new Flutation(env, entry.getKey());
        for (Column col : entry.getValue()) {
          m.put(ColumnConstants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(col), gv(col), startTs, TransactionImpl.EMPTY);
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
    Map<Bytes,Set<Column>> columnsToRead = new HashMap<>();
    
    for (Entry<Bytes,Set<Column>> entry : cd.getRejected().entrySet()) {
      Set<Column> rowColsRead = columnsRead.get(entry.getKey());
      if (rowColsRead == null) {
        columnsToRead.put(entry.getKey(), entry.getValue());
      } else {
        HashSet<Column> colsToRead = new HashSet<>(entry.getValue());
        colsToRead.removeAll(rowColsRead);
        if (colsToRead.size() > 0) {
          columnsToRead.put(entry.getKey(), colsToRead);
        }
      }
    }

    for (Entry<Bytes,Set<Column>> entry : columnsToRead.entrySet()) {
      getImpl(entry.getKey(), entry.getValue());
    }
  }

  private boolean checkForAckCollision(ConditionalMutation cm) {
    Bytes row = Bytes.wrap(cm.getRow());

    if (row.equals(triggerRow)) {
      List<ColumnUpdate> updates = cm.getUpdates();
      
      for (ColumnUpdate cu : updates) {
        // TODO avoid create col vis object
        Column col = new Column(Bytes.wrap(cu.getColumnFamily()), Bytes.wrap(cu.getColumnQualifier())).setVisibility(Bytes.wrap(cu.getColumnVisibility()));

        if (triggerColumn.equals(col)) {
          
          // TODO this check will not detect ack when tx overlaps with another tx... it will instead the the lock release.. this may be ok, the worker will
          // retry the tx and then see the already ack exception

          IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
          PrewriteIterator.setSnaptime(iterConf, startTs);
          PrewriteIterator.enableAckCheck(iterConf);
          Key key = ColumnUtil.checkColumn(env, iterConf, row, col).getKey();
          // TODO could key be null?
          long colType = key.getTimestamp() & ColumnConstants.PREFIX_MASK;
          
          if (colType == ColumnConstants.ACK_PREFIX) {
            return true;
          }
        }
      }
    }
    
    return false;
  }

  public boolean commitPrimaryColumn(CommitData cd, long commitTs) throws AccumuloException, AccumuloSecurityException {
    // try to delete lock and add write for primary column
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    boolean isTrigger = cd.prow.equals(triggerRow) && cd.pcol.equals(triggerColumn);

    Condition lockCheck = new FluoCondition(env, cd.pcol).setIterators(iterConf).setValue(
        LockValue.encode(cd.prow, cd.pcol, cd.pval != null, cd.pval == DELETE, isTrigger, getTransactorID()));
    ConditionalMutation delLockMutation = new ConditionalFlutation(env, cd.prow, lockCheck);

    ColumnUtil.commitColumn(env, isTrigger, true, cd.pcol, cd.pval != null, cd.pval == DELETE, startTs, commitTs, observedColumns, delLockMutation);
    
    Status mutationStatus = cd.cw.write(delLockMutation).getStatus();
    
    while (mutationStatus == Status.UNKNOWN) {
      
      TxInfo txInfo = TxInfo.getTransactionInfo(env, cd.prow, cd.pcol, startTs);
      
      switch (txInfo.status) {
        case COMMITTED:
          if (txInfo.commitTs != commitTs)
            throw new IllegalStateException(cd.prow + " " + cd.pcol + " " + txInfo.commitTs + "!=" + commitTs);
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
    
    Flutation m;

    ArrayList<Mutation> mutations = new ArrayList<>(cd.acceptedRows.size());
    for (Bytes row : cd.acceptedRows) {
      m = new Flutation(env, row);
      for (Column col : updates.get(row).keySet()) {
        m.put(col, ColumnConstants.DEL_LOCK_PREFIX | startTs, DelLockValue.encode(startTs, false, true));
      }
      mutations.add(m);
    }
    
    env.getSharedResources().getBatchWriter().writeMutations(mutations);
    
    // mark transaction as complete for garbage collection purposes
    m = new Flutation(env, cd.prow);
    // TODO timestamp?
    // TODO writing the primary column with a batch writer is iffy

    m.put(cd.pcol, ColumnConstants.DEL_LOCK_PREFIX | startTs, DelLockValue.encode(startTs, false, true));
    m.put(cd.pcol, ColumnConstants.TX_DONE_PREFIX | startTs, EMPTY);

    env.getSharedResources().getBatchWriter().writeMutation(m);
  }
  
  public boolean finishCommit(CommitData cd, long commitTs) throws TableNotFoundException, MutationsRejectedException {
    // delete locks and add writes for other columns
    ArrayList<Mutation> mutations = new ArrayList<>(updates.size() + 1);
    for (Entry<Bytes,Map<Column,Bytes>> rowUpdates : updates.entrySet()) {
      Flutation m = new Flutation(env, rowUpdates.getKey());
      boolean isTriggerRow = rowUpdates.getKey().equals(triggerRow);
      for (Entry<Column,Bytes> colUpdates : rowUpdates.getValue().entrySet()) {
        ColumnUtil.commitColumn(env, isTriggerRow && colUpdates.getKey().equals(triggerColumn), false, colUpdates.getKey(), colUpdates.getValue() != null,
            colUpdates.getValue() == DELETE, startTs, commitTs, observedColumns, m);
      }
      
      mutations.add(m);
    }
    
    if (weakRow != null) {
      Flutation m = new Flutation(env, weakRow);
      m.putDelete(ColumnConstants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(weakColumn), gv(weakColumn), commitTs);

      mutations.add(m);
    }

    env.getSharedResources().getBatchWriter().writeMutations(mutations);
    
    // mark transaction as complete for garbage collection purposes

    Flutation m = new Flutation(env, cd.prow);
    m.put(cd.pcol, ColumnConstants.TX_DONE_PREFIX | commitTs, EMPTY);

    env.getSharedResources().getBatchWriter().writeMutationAsync(m);
    
    return true;
  }

  public CommitData createCommitData() {
    CommitData cd = new CommitData();
    cd.cw = env.getSharedResources().getConditionalWriter();
    return cd;
  }

  @Override
  public synchronized void commit() throws CommitException {
    
    if (status == TxStatus.CLOSED) {
      throw new CommitException("Transaction was previously closed");
    } else if (status == TxStatus.COMMITTED) {
      throw new CommitException("Transaction was previously committed");
    }
    
    if (updates.size() == 0) {
      deleteWeakRow();
      stats.setFinishTime(System.currentTimeMillis());
      return;
    }

    for (Map<Column,Bytes> cols : updates.values())
      stats.incrementEntriesSet(cols.size());

    CommitData cd = createCommitData();
    
    try {
      if (!preCommit(cd)) {
        readUnread(cd);
        throw new CommitException("Pre-commit failed");
      }
      
      long commitTs = OracleClient.getInstance(env).getTimestamp();
      if (commitPrimaryColumn(cd, commitTs)) {
        finishCommit(cd, commitTs);
      } else {
        // TODO write TX_DONE
        throw new CommitException("Commit failed");
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
      status = TxStatus.COMMITTED;
    }
  }

  void deleteWeakRow() {
    if (weakRow != null) {
      long commitTs;
      try {
        commitTs = OracleClient.getInstance(env).getTimestamp();
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      Flutation m = new Flutation(env, weakRow);
      m.putDelete(ColumnConstants.NOTIFY_CF.toArray(), ColumnUtil.concatCFCQ(weakColumn), gv(weakColumn), commitTs);

      env.getSharedResources().getBatchWriter().writeMutation(m);
    }
  }

  public TxStats getStats() {
    return stats;
  }

  public long getStartTs() {
    return startTs;
  }
  
  /**
   * Sets the transactor of this transaction
   * 
   * @param transactor
   * @return this Transaction
   */
  @VisibleForTesting
  public TransactionImpl setTransactor(TransactorNode tnode) {
    this.tnode = tnode;
    return this;
  }
  
  /**
   * Retrieves transactor ID by first getting/creating
   * transactor (which is only done until necessary)
   */
  private Long getTransactorID() {
    if (tnode == null) {
      tnode = env.getSharedResources().getTransactorNode();
    }
    return tnode.getTransactorID().getLongID();
  }

  @Override
  public synchronized void close() {
    if (status != TxStatus.CLOSED) {
      status = TxStatus.CLOSED;
      env.getSharedResources().getTimestampTracker().removeTimestamp(startTs);
    }
  }
  
  private synchronized void checkIfOpen() {
    if (status != TxStatus.OPEN) {
      throw new IllegalStateException("Transation is no longer open! status = " + status);
    }
  }

  @Override
  protected void finalize() throws Throwable {
    close();
  }
}
