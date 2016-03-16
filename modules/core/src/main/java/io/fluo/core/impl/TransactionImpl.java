/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import io.fluo.core.exceptions.StaleScanException;
import io.fluo.core.oracle.Stamp;
import io.fluo.core.util.ColumnUtil;
import io.fluo.core.util.ConditionalFlutation;
import io.fluo.core.util.FluoCondition;
import io.fluo.core.util.Flutation;
import io.fluo.core.util.SpanUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Result;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;

/**
 * Transaction implementation
 */
public class TransactionImpl implements Transaction, Snapshot {

  public static final byte[] EMPTY = new byte[0];
  public static final Bytes EMPTY_BS = Bytes.of(EMPTY);
  private static final Bytes DELETE = Bytes.of("special delete object");

  private static enum TxStatus {
    OPEN, COMMIT_STARTED, COMMITTED, CLOSED
  }

  private final long startTs;
  private final Map<Bytes, Map<Column, Bytes>> updates = new HashMap<>();
  private final Map<Bytes, Set<Column>> weakNotifications = new HashMap<>();
  private final Set<Column> observedColumns;
  private final Environment env;
  final Map<Bytes, Set<Column>> columnsRead = new HashMap<>();
  private final TxStats stats;
  private Notification notification;
  private Notification weakNotification;
  private TransactorNode tnode = null;
  private TxStatus status = TxStatus.OPEN;
  private boolean commitAttempted = false;

  public TransactionImpl(Environment env, Notification trigger, long startTs) {
    Objects.requireNonNull(env, "environment cannot be null");
    Preconditions.checkArgument(startTs >= 0, "startTs cannot be negative");
    this.env = env;
    this.stats = new TxStats(env);
    this.startTs = startTs;
    this.observedColumns = env.getObservers().keySet();

    if (trigger != null && env.getWeakObservers().containsKey(trigger.getColumn())) {
      this.weakNotification = trigger;
    } else {
      this.notification = trigger;
    }

    if (notification != null) {
      Map<Column, Bytes> colUpdates = new HashMap<>();
      colUpdates.put(notification.getColumn(), null);
      updates.put(notification.getRow(), colUpdates);
    }
  }

  public TransactionImpl(Environment env, Notification trigger) {
    this(env, trigger, allocateTimestamp(env).getTxTimestamp());
  }

  public TransactionImpl(Environment env) {
    this(env, null, allocateTimestamp(env).getTxTimestamp());
  }

  public TransactionImpl(Environment env, long startTs) {
    this(env, null, startTs);
  }

  private static Stamp allocateTimestamp(Environment env) {
    return env.getSharedResources().getTimestampTracker().allocateTimestamp();
  }

  @Override
  public Bytes get(Bytes row, Column column) {
    checkIfOpen();
    // TODO cache? precache?
    return get(row, Collections.singleton(column)).get(column);
  }

  @Override
  public Map<Column, Bytes> get(Bytes row, Set<Column> columns) {
    checkIfOpen();
    return getImpl(row, columns);
  }

  @Override
  public Map<Bytes, Map<Column, Bytes>> get(Collection<Bytes> rows, Set<Column> columns) {
    checkIfOpen();

    if (rows.size() == 0 || columns.size() == 0) {
      return Collections.emptyMap();
    }

    env.getSharedResources().getVisCache().validate(columns);

    ParallelSnapshotScanner pss = new ParallelSnapshotScanner(rows, columns, env, startTs, stats);

    Map<Bytes, Map<Column, Bytes>> ret = pss.scan();

    for (Entry<Bytes, Map<Column, Bytes>> entry : ret.entrySet()) {
      updateColumnsRead(entry.getKey(), entry.getValue().keySet());
    }

    return ret;
  }

  // TODO add a get that uses the batch scanner

  @Override
  public RowIterator get(ScannerConfiguration config) {
    checkIfOpen();
    return getImpl(config);
  }

  private Map<Column, Bytes> getImpl(Bytes row, Set<Column> columns) {

    // TODO push visibility filtering to server side?

    env.getSharedResources().getVisCache().validate(columns);

    ScannerConfiguration config = new ScannerConfiguration();
    config.setSpan(Span.exact(row));
    for (Column column : columns) {
      config.fetchColumn(column.getFamily(), column.getQualifier());
    }

    RowIterator iter = getImpl(config);

    Map<Column, Bytes> ret = new HashMap<>();

    while (iter.hasNext()) {
      Entry<Bytes, ColumnIterator> entry = iter.next();
      ColumnIterator citer = entry.getValue();
      while (citer.hasNext()) {
        Entry<Column, Bytes> centry = citer.next();
        if (columns.contains(centry.getKey())) {
          ret.put(centry.getKey(), centry.getValue());
        }
      }
    }

    // only update columns read after successful read
    updateColumnsRead(row, columns);

    return ret;
  }

  private RowIterator getImpl(ScannerConfiguration config) {
    return new RowIteratorImpl(new SnapshotScanner(this.env, config, startTs, stats));
  }

  private void updateColumnsRead(Bytes row, Set<Column> columns) {
    Set<Column> colsRead = columnsRead.get(row);
    if (colsRead == null) {
      colsRead = new HashSet<>();
      columnsRead.put(row, colsRead);
    }
    colsRead.addAll(columns);
  }

  @Override
  public void set(Bytes row, Column col, Bytes value) throws AlreadySetException {
    checkIfOpen();
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);
    Objects.requireNonNull(value);

    if (col.getFamily().equals(ColumnConstants.NOTIFY_CF)) {
      throw new IllegalArgumentException(ColumnConstants.NOTIFY_CF + " is a reserved family");
    }

    env.getSharedResources().getVisCache().validate(col);

    // TODO copy?

    Map<Column, Bytes> colUpdates = updates.get(row);
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
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);

    if (!env.getWeakObservers().containsKey(col)) {
      throw new IllegalArgumentException("Column not configured for weak notifications " + col);
    }

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
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);
    set(row, col, DELETE);
  }

  private ConditionalFlutation prewrite(ConditionalFlutation cm, Bytes row, Column col, Bytes val,
      Bytes primaryRow, Column primaryColumn, boolean isTriggerRow) {
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    boolean isTrigger = isTriggerRow && col.equals(notification.getColumn());
    if (isTrigger) {
      PrewriteIterator.enableAckCheck(iterConf, notification.getTimestamp());
    }

    Condition cond = new FluoCondition(env, col).setIterators(iterConf);

    if (cm == null) {
      cm = new ConditionalFlutation(env, row, cond);
    } else {
      cm.addCondition(cond);
    }

    if (val != null && val != DELETE) {
      cm.put(col, ColumnConstants.DATA_PREFIX | startTs, val.toArray());
    }

    cm.put(col, ColumnConstants.LOCK_PREFIX | startTs, LockValue.encode(primaryRow, primaryColumn,
        val != null, val == DELETE, isTriggerRow, getTransactorID()));

    return cm;
  }

  private ConditionalFlutation prewrite(Bytes row, Column col, Bytes val, Bytes primaryRow,
      Column primaryColumn, boolean isTriggerRow) {
    return prewrite(null, row, col, val, primaryRow, primaryColumn, isTriggerRow);
  }

  private void prewrite(ConditionalFlutation cm, Column col, Bytes val, Bytes primaryRow,
      Column primaryColumn, boolean isTriggerRow) {
    prewrite(cm, null, col, val, primaryRow, primaryColumn, isTriggerRow);
  }

  public static class CommitData {
    ConditionalWriter cw;
    ConditionalWriter bulkCw;
    private Bytes prow;
    private Column pcol;
    private Bytes pval;

    private HashSet<Bytes> acceptedRows;
    private Map<Bytes, Set<Column>> rejected = new HashMap<>();

    private void addPrimaryToRejected() {
      rejected = Collections.singletonMap(prow, Collections.singleton(pcol));
    }

    private void addToRejected(Bytes row, Set<Column> columns) {
      rejected = new HashMap<>();

      Set<Column> ret = rejected.put(row, columns);
      if (ret != null) {
        throw new IllegalStateException();
      }
    }

    private Map<Bytes, Set<Column>> getRejected() {
      if (rejected == null) {
        return Collections.emptyMap();
      }

      return rejected;
    }

    @Override
    public String toString() {
      return prow + " " + pcol + " " + pval + " " + rejected.size();
    }
  }

  private boolean isTriggerRow(Bytes row) {
    return notification != null && notification.getRow().equals(row);
  }

  public boolean preCommit(CommitData cd) throws TableNotFoundException, AccumuloException,
      AccumuloSecurityException, AlreadyAcknowledgedException {
    if (notification != null) {
      // always want to throw already ack exception if collision, so process trigger first
      return preCommit(cd, notification.getRow(), notification.getColumn());
    } else {
      Bytes prow = updates.keySet().iterator().next();
      Map<Column, Bytes> colSet = updates.get(prow);
      Column pcol = colSet.keySet().iterator().next();
      return preCommit(cd, prow, pcol);
    }

  }

  public boolean preCommit(CommitData cd, Bytes primRow, Column primCol)
      throws TableNotFoundException, AccumuloException, AccumuloSecurityException,
      AlreadyAcknowledgedException {

    checkIfOpen();
    status = TxStatus.COMMIT_STARTED;

    // get a primary column
    cd.prow = primRow;
    Map<Column, Bytes> colSet = updates.get(cd.prow);
    cd.pcol = primCol;
    cd.pval = colSet.remove(primCol);
    if (colSet.size() == 0) {
      updates.remove(cd.prow);
    }

    // try to lock primary column
    ConditionalMutation pcm =
        prewrite(cd.prow, cd.pcol, cd.pval, cd.prow, cd.pcol, isTriggerRow(cd.prow));

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
          throw new IllegalStateException("unexpected tx state " + txInfo.status + " " + cd.prow
              + " " + cd.pcol);

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

    int numUpdates = 0;
    for (Entry<Bytes, Map<Column, Bytes>> rowUpdates : updates.entrySet()) {
      ConditionalFlutation cm = null;
      boolean isTriggerRow = isTriggerRow(rowUpdates.getKey());

      for (Entry<Column, Bytes> colUpdates : rowUpdates.getValue().entrySet()) {
        if (cm == null) {
          cm =
              prewrite(rowUpdates.getKey(), colUpdates.getKey(), colUpdates.getValue(), cd.prow,
                  cd.pcol, isTriggerRow);
        } else {
          prewrite(cm, colUpdates.getKey(), colUpdates.getValue(), cd.prow, cd.pcol, isTriggerRow);
        }
      }

      mutations.add(cm);

      numUpdates += rowUpdates.getValue().size();
    }

    cd.acceptedRows = new HashSet<>();

    boolean ackCollision = false;

    Iterator<Result> resultsIter;

    if (numUpdates < 10) {
      resultsIter = cd.cw.write(mutations.iterator());
    } else {
      resultsIter = cd.bulkCw.write(mutations.iterator());
    }

    while (resultsIter.hasNext()) {
      Result result = resultsIter.next();
      // TODO handle unknown?
      Bytes row = Bytes.of(result.getMutation().getRow());
      if (result.getStatus() == Status.ACCEPTED) {
        cd.acceptedRows.add(row);
      } else {
        // TODO if trigger is always primary row:col, then do not need checks elsewhere
        ackCollision |= checkForAckCollision(result.getMutation());
        cd.addToRejected(row, updates.get(row).keySet());
      }
    }

    if (cd.getRejected().size() > 0) {
      rollback(cd);

      if (ackCollision) {
        throw new AlreadyAcknowledgedException();
      }

      return false;
    }

    return true;
  }

  private void writeWeakNotifications(long commitTs) {
    if (weakNotifications.size() > 0) {
      SharedBatchWriter sbw = env.getSharedResources().getBatchWriter();
      ArrayList<Mutation> mutations = new ArrayList<>();

      for (Entry<Bytes, Set<Column>> entry : weakNotifications.entrySet()) {
        Flutation m = new Flutation(env, entry.getKey());
        for (Column col : entry.getValue()) {
          Notification.put(env, m, col, commitTs);
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
   * <p>
   * In this case TX2 would not roll back TX1, because it never read the column. This function
   * attempts to handle this case if TX2 fails. Only doing this in case of failures is cheaper than
   * trying to always read unread columns.
   *
   * @param cd Commit data
   */
  private void readUnread(CommitData cd) throws Exception {
    // TODO need to keep track of ranges read (not ranges passed in, but actual data read... user
    // may not iterate over entire range
    Map<Bytes, Set<Column>> columnsToRead = new HashMap<>();

    for (Entry<Bytes, Set<Column>> entry : cd.getRejected().entrySet()) {
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

    for (Entry<Bytes, Set<Column>> entry : columnsToRead.entrySet()) {
      getImpl(entry.getKey(), entry.getValue());
    }
  }

  private boolean checkForAckCollision(ConditionalMutation cm) {
    Bytes row = Bytes.of(cm.getRow());

    if (isTriggerRow(row)) {
      List<ColumnUpdate> updates = cm.getUpdates();

      for (ColumnUpdate cu : updates) {
        // TODO avoid create col vis object
        Column col =
            new Column(Bytes.of(cu.getColumnFamily()), Bytes.of(cu.getColumnQualifier()),
                Bytes.of(cu.getColumnVisibility()));

        if (notification.getColumn().equals(col)) {
          // check to see if ACK exist after notification
          Key startKey = SpanUtil.toKey(notification);
          startKey.setTimestamp(ColumnConstants.ACK_PREFIX
              | (Long.MAX_VALUE & ColumnConstants.TIMESTAMP_MASK));

          Key endKey = SpanUtil.toKey(notification);
          endKey.setTimestamp(ColumnConstants.ACK_PREFIX | (notification.getTimestamp() + 1));

          Range range = new Range(startKey, endKey);

          Scanner scanner;
          try {
            // TODO reuse or share scanner
            scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
          } catch (TableNotFoundException e) {
            // TODO proper exception handling
            throw new RuntimeException(e);
          }

          scanner.setRange(range);

          // TODO could use iterator that stops after 1st ACK. thought of using versioning iter but
          // it scans to ACK
          if (scanner.iterator().hasNext()) {
            env.getSharedResources().getBatchWriter()
                .writeMutationAsync(notification.newDelete(env));
            return true;
          }
        }
      }
    }

    return false;
  }

  public boolean commitPrimaryColumn(CommitData cd, Stamp commitStamp) throws AccumuloException,
      AccumuloSecurityException {
    if (startTs < commitStamp.getGcTimestamp()) {
      rollback(cd);
      return false;
    }

    return commitPrimaryColumn(cd, commitStamp.getTxTimestamp());
  }

  public boolean commitPrimaryColumn(CommitData cd, long commitTs) throws AccumuloException,
      AccumuloSecurityException {
    // set weak notifications after all locks are written, but before finishing commit. If weak
    // notifications were set after the commit, then information
    // about weak notifications would need to be persisted in the lock phase. Setting here is safe
    // because any observers that run as a result of the weak
    // notification will wait for the commit to finish. Setting here may cause an observer to run
    // unnecessarily in the case of rollback, but that is ok.
    // TODO look into setting weak notification as part of lock and commit phases to avoid this
    // synchronous step
    writeWeakNotifications(commitTs);

    // try to delete lock and add write for primary column
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    boolean isTrigger = isTriggerRow(cd.prow) && cd.pcol.equals(notification.getColumn());

    Condition lockCheck =
        new FluoCondition(env, cd.pcol).setIterators(iterConf).setValue(
            LockValue.encode(cd.prow, cd.pcol, cd.pval != null, cd.pval == DELETE, isTrigger,
                getTransactorID()));
    ConditionalMutation delLockMutation = new ConditionalFlutation(env, cd.prow, lockCheck);

    ColumnUtil.commitColumn(env, isTrigger, true, cd.pcol, cd.pval != null, cd.pval == DELETE,
        startTs, commitTs, observedColumns, delLockMutation);

    Status mutationStatus = cd.cw.write(delLockMutation).getStatus();

    while (mutationStatus == Status.UNKNOWN) {

      TxInfo txInfo = TxInfo.getTransactionInfo(env, cd.prow, cd.pcol, startTs);

      switch (txInfo.status) {
        case COMMITTED:
          if (txInfo.commitTs != commitTs) {
            throw new IllegalStateException(cd.prow + " " + cd.pcol + " " + txInfo.commitTs + "!="
                + commitTs);
          }
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

  private void rollback(CommitData cd) throws MutationsRejectedException {
    // roll back locks

    // TODO let rollback be done lazily? this makes GC more difficult

    Flutation m;

    ArrayList<Mutation> mutations = new ArrayList<>(cd.acceptedRows.size());
    for (Bytes row : cd.acceptedRows) {
      m = new Flutation(env, row);
      for (Column col : updates.get(row).keySet()) {
        m.put(col, ColumnConstants.DEL_LOCK_PREFIX | startTs,
            DelLockValue.encodeRollback(false, true));
      }
      mutations.add(m);
    }

    env.getSharedResources().getBatchWriter().writeMutations(mutations);

    // mark transaction as complete for garbage collection purposes
    m = new Flutation(env, cd.prow);
    // TODO timestamp?
    // TODO writing the primary column with a batch writer is iffy

    m.put(cd.pcol, ColumnConstants.DEL_LOCK_PREFIX | startTs,
        DelLockValue.encodeRollback(startTs, false, true));
    m.put(cd.pcol, ColumnConstants.TX_DONE_PREFIX | startTs, EMPTY);

    env.getSharedResources().getBatchWriter().writeMutation(m);
  }

  public boolean finishCommit(CommitData cd, Stamp commitStamp) throws TableNotFoundException,
      MutationsRejectedException {
    long commitTs = commitStamp.getTxTimestamp();
    // delete locks and add writes for other columns
    ArrayList<Mutation> mutations = new ArrayList<>(updates.size() + 1);
    for (Entry<Bytes, Map<Column, Bytes>> rowUpdates : updates.entrySet()) {
      Flutation m = new Flutation(env, rowUpdates.getKey());
      boolean isTriggerRow = isTriggerRow(rowUpdates.getKey());
      for (Entry<Column, Bytes> colUpdates : rowUpdates.getValue().entrySet()) {
        ColumnUtil.commitColumn(env,
            isTriggerRow && colUpdates.getKey().equals(notification.getColumn()), false,
            colUpdates.getKey(), colUpdates.getValue() != null, colUpdates.getValue() == DELETE,
            startTs, commitTs, observedColumns, m);
      }

      mutations.add(m);
    }

    ArrayList<Mutation> afterFlushMutations = new ArrayList<>(2);

    Flutation m = new Flutation(env, cd.prow);
    // mark transaction as complete for garbage collection purposes
    m.put(cd.pcol, ColumnConstants.TX_DONE_PREFIX | commitTs, EMPTY);
    afterFlushMutations.add(m);

    if (weakNotification != null) {
      afterFlushMutations.add(weakNotification.newDelete(env, startTs));
    }

    if (notification != null) {
      afterFlushMutations.add(notification.newDelete(env, startTs));
    }

    env.getSharedResources().getBatchWriter().writeMutationsAsync(mutations, afterFlushMutations);

    return true;
  }

  public CommitData createCommitData() {
    CommitData cd = new CommitData();
    cd.cw = env.getSharedResources().getConditionalWriter();
    cd.bulkCw = env.getSharedResources().getBulkConditionalWriter();
    return cd;
  }

  @Override
  public synchronized void commit() throws CommitException {

    if (status == TxStatus.CLOSED) {
      throw new CommitException("Transaction was previously closed");
    } else if (status == TxStatus.COMMITTED) {
      throw new CommitException("Transaction was previously committed");
    }

    commitAttempted = true;

    if (updates.size() == 0) {
      deleteWeakRow();
      stats.setFinishTime(System.currentTimeMillis());
      return;
    }

    for (Map<Column, Bytes> cols : updates.values()) {
      stats.incrementEntriesSet(cols.size());
    }

    CommitData cd = createCommitData();

    try {
      long t1 = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
      if (!preCommit(cd)) {
        readUnread(cd);
        throw new CommitException("Pre-commit failed");
      }
      long t2 = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());

      Stamp commitStamp = env.getSharedResources().getOracleClient().getStamp();
      if (commitPrimaryColumn(cd, commitStamp)) {
        long t3 = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        stats.setCommitTs(commitStamp.getTxTimestamp());
        finishCommit(cd, commitStamp);
        long t4 = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
        stats.setCommitTimes(t2 - t1, t3 - t2, t4 - t3);
      } else {
        // TODO write TX_DONE (done in case where startTs < gcTimestamp)
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
      stats.setRejected(cd.getRejected());
      status = TxStatus.COMMITTED;
    }
  }

  void deleteWeakRow() {
    if (weakNotification != null) {
      env.getSharedResources().getBatchWriter()
          .writeMutation(weakNotification.newDelete(env, startTs));
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
   * @param tnode TransactorNode
   * @return this Transaction
   */
  @VisibleForTesting
  public TransactionImpl setTransactor(TransactorNode tnode) {
    this.tnode = tnode;
    return this;
  }

  /**
   * Retrieves transactor ID by first getting/creating transactor (which is only done until
   * necessary)
   */
  private Long getTransactorID() {
    if (tnode == null) {
      tnode = env.getSharedResources().getTransactorNode();
    }
    return tnode.getTransactorID().getLongID();
  }

  private synchronized void close(boolean checkForStaleScan) {
    if (status != TxStatus.CLOSED) {
      status = TxStatus.CLOSED;

      if (checkForStaleScan && !commitAttempted) {
        Stamp stamp = env.getSharedResources().getOracleClient().getStamp();
        if (startTs < stamp.getGcTimestamp()) {
          throw new StaleScanException();
        }
      }

      env.getSharedResources().getTimestampTracker().removeTimestamp(startTs);
    }
  }

  @Override
  public void close() {
    close(true);
  }

  private synchronized void checkIfOpen() {
    if (status != TxStatus.OPEN) {
      throw new IllegalStateException("Transaction is no longer open! status = " + status);
    }
  }

  // CHECKSTYLE:OFF
  @Override
  protected void finalize() throws Throwable {
    // CHECKSTYLE:ON
    // TODO Log an error if transaction is not closed (See FLUO-486)
    close(false);
  }

  @Override
  public long getStartTimestamp() {
    return startTs;
  }
}
