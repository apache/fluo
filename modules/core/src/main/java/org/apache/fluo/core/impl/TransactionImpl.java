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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.iterators.PrewriteIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.values.DelLockValue;
import org.apache.fluo.accumulo.values.LockValue;
import org.apache.fluo.api.client.AbstractTransactionBase;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.ScannerBuilder;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.AlreadySetException;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.core.async.AsyncCommitObserver;
import org.apache.fluo.core.async.AsyncConditionalWriter;
import org.apache.fluo.core.async.AsyncTransaction;
import org.apache.fluo.core.async.SyncCommitObserver;
import org.apache.fluo.core.exceptions.AlreadyAcknowledgedException;
import org.apache.fluo.core.exceptions.StaleScanException;
import org.apache.fluo.core.impl.scanner.ScannerBuilderImpl;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.core.util.ColumnUtil;
import org.apache.fluo.core.util.ConditionalFlutation;
import org.apache.fluo.core.util.FluoCondition;
import org.apache.fluo.core.util.Flutation;
import org.apache.fluo.core.util.Hex;
import org.apache.fluo.core.util.SpanUtil;

import static org.apache.fluo.api.observer.Observer.NotificationType.STRONG;
import static org.apache.fluo.api.observer.Observer.NotificationType.WEAK;

/**
 * Transaction implementation
 */
public class TransactionImpl extends AbstractTransactionBase implements AsyncTransaction, Snapshot {

  public static final byte[] EMPTY = new byte[0];
  public static final Bytes EMPTY_BS = Bytes.of(EMPTY);
  private static final Bytes DELETE = Bytes
      .of("special delete object f804266bf94935edd45ae3e6c287b93c1814295c");
  private static final Bytes NTFY_VAL = Bytes
      .of("special ntfy value ce0c523e6e4dc093be8a2736b82eca1b95f97ed4");

  private static boolean isWrite(Bytes val) {
    return val != NTFY_VAL;
  }

  private static boolean isDelete(Bytes val) {
    return val == DELETE;
  }

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

  // for testing
  private boolean stopAfterPreCommit = false;
  private boolean stopAfterPrimaryCommit = false;

  public TransactionImpl(Environment env, Notification trigger, long startTs) {
    Objects.requireNonNull(env, "environment cannot be null");
    Preconditions.checkArgument(startTs >= 0, "startTs cannot be negative");
    this.env = env;
    this.stats = new TxStats(env);
    this.startTs = startTs;
    this.observedColumns = env.getConfiguredObservers().getObservedColumns(STRONG);

    if (trigger != null
        && env.getConfiguredObservers().getObservedColumns(WEAK).contains(trigger.getColumn())) {
      this.weakNotification = trigger;
    } else {
      this.notification = trigger;
    }

    if (notification != null) {
      Map<Column, Bytes> colUpdates = new HashMap<>();
      colUpdates.put(notification.getColumn(), NTFY_VAL);
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

  @Override
  public Map<RowColumn, Bytes> get(Collection<RowColumn> rowColumns) {
    checkIfOpen();

    if (rowColumns.size() == 0) {
      return Collections.emptyMap();
    }

    ParallelSnapshotScanner pss = new ParallelSnapshotScanner(rowColumns, env, startTs, stats);

    Map<Bytes, Map<Column, Bytes>> scan = pss.scan();
    Map<RowColumn, Bytes> ret = new HashMap<>();

    for (Entry<Bytes, Map<Column, Bytes>> entry : scan.entrySet()) {
      updateColumnsRead(entry.getKey(), entry.getValue().keySet());
      for (Entry<Column, Bytes> colVal : entry.getValue().entrySet()) {
        ret.put(new RowColumn(entry.getKey(), colVal.getKey()), colVal.getValue());
      }
    }

    return ret;
  }

  private Map<Column, Bytes> getImpl(Bytes row, Set<Column> columns) {

    // TODO push visibility filtering to server side?

    env.getSharedResources().getVisCache().validate(columns);

    boolean shouldCopy = false;

    for (Column column : columns) {
      if (column.isVisibilitySet()) {
        shouldCopy = true;
      }
    }

    SnapshotScanner.Opts opts;
    if (shouldCopy) {
      HashSet<Column> cols = new HashSet<>();
      for (Column column : columns) {
        if (column.isVisibilitySet()) {
          cols.add(new Column(column.getFamily(), column.getQualifier()));
        } else {
          cols.add(column);
        }
      }
      opts = new SnapshotScanner.Opts(Span.exact(row), columns);
    } else {
      opts = new SnapshotScanner.Opts(Span.exact(row), columns);
    }

    Map<Column, Bytes> ret = new HashMap<>();

    for (Entry<Key, Value> kve : new SnapshotScanner(env, opts, startTs, stats)) {
      Column col = ColumnUtil.convert(kve.getKey());
      if (shouldCopy) {
        if (columns.contains(col)) {
          ret.put(col, Bytes.of(kve.getValue().get()));
        }
      } else {
        ret.put(col, Bytes.of(kve.getValue().get()));
      }
    }

    // only update columns read after successful read
    updateColumnsRead(row, columns);

    return ret;
  }

  @Override
  public ScannerBuilder scanner() {
    checkIfOpen();
    return new ScannerBuilderImpl(this);
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

    Bytes curVal = colUpdates.get(col);
    if (curVal != null && isWrite(curVal)) {
      throw new AlreadySetException("Value already set " + row + " " + col);
    }
    colUpdates.put(col, value);
  }

  @Override
  public void setWeakNotification(Bytes row, Column col) {
    checkIfOpen();
    Objects.requireNonNull(row);
    Objects.requireNonNull(col);

    if (!env.getConfiguredObservers().getObservedColumns(WEAK).contains(col)) {
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

    if (isWrite(val) && !isDelete(val)) {
      cm.put(col, ColumnConstants.DATA_PREFIX | startTs, val.toArray());
    }

    cm.put(col, ColumnConstants.LOCK_PREFIX | startTs, LockValue.encode(primaryRow, primaryColumn,
        isWrite(val), isDelete(val), isTriggerRow, getTransactorID()));

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
    private Bytes prow;
    private Column pcol;
    private Bytes pval;

    private HashSet<Bytes> acceptedRows;
    private Map<Bytes, Set<Column>> rejected = null;

    private void addPrimaryToRejected() {
      rejected = Collections.singletonMap(prow, Collections.singleton(pcol));
    }

    private void addToRejected(Bytes row, Set<Column> columns) {
      if (rejected == null) {
        rejected = new HashMap<>();
      }

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
      return prow + " " + pcol + " " + pval + " " + getRejected().size();
    }

    public String getShortCollisionMessage() {
      StringBuilder sb = new StringBuilder();
      if (getRejected().size() > 0) {
        int numCollisions = 0;
        for (Set<Column> cols : getRejected().values()) {
          numCollisions += cols.size();
        }

        sb.append("Collisions(");
        sb.append(numCollisions);
        sb.append("):");

        String sep = "";
        outer: for (Entry<Bytes, Set<Column>> entry : getRejected().entrySet()) {
          Bytes row = entry.getKey();
          for (Column col : entry.getValue()) {
            sb.append(sep);
            sep = ", ";
            Hex.encNonAscii(sb, row);
            sb.append(" ");
            Hex.encNonAscii(sb, col, " ");
            if (sb.length() > 100) {
              sb.append(" ...");
              break outer;
            }
          }
        }
      }

      return sb.toString();
    }

    // async stuff
    private AsyncConditionalWriter acw;
    private AsyncConditionalWriter bacw;
    private AsyncCommitObserver commitObserver;

  }

  private boolean isTriggerRow(Bytes row) {
    return notification != null && notification.getRow().equals(row);
  }

  public boolean preCommit(CommitData cd) {
    return preCommit(cd, null);
  }

  @VisibleForTesting
  public boolean preCommit(CommitData cd, RowColumn primary) {

    synchronized (this) {
      checkIfOpen();
      status = TxStatus.COMMIT_STARTED;
      commitAttempted = true;

      stopAfterPreCommit = true;
    }

    SyncCommitObserver sco = new SyncCommitObserver();
    beginCommitAsync(cd, sco, primary);
    try {
      sco.waitForCommit();
    } catch (AlreadyAcknowledgedException e) {
      throw e;
    } catch (CommitException e) {
      return false;
    }
    return true;
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
    // TODO make async
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
          Key startKey = SpanUtil.toKey(notification.getRowColumn());
          startKey.setTimestamp(ColumnConstants.ACK_PREFIX
              | (Long.MAX_VALUE & ColumnConstants.TIMESTAMP_MASK));

          Key endKey = SpanUtil.toKey(notification.getRowColumn());
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

  @VisibleForTesting
  public boolean commitPrimaryColumn(CommitData cd, Stamp commitStamp) {
    stopAfterPrimaryCommit = true;

    SyncCommitObserver sco = new SyncCommitObserver();
    cd.commitObserver = sco;
    try {
      beginSecondCommitPhase(cd, commitStamp);
      sco.waitForCommit();
    } catch (CommitException e) {
      return false;
    } catch (Exception e) {
      throw new FluoException(e);
    }
    return true;
  }

  public CommitData createCommitData() {
    CommitData cd = new CommitData();
    cd.cw = env.getSharedResources().getConditionalWriter();
    cd.acw = env.getSharedResources().getAsyncConditionalWriter();
    cd.bacw = env.getSharedResources().getBulkAsyncConditionalWriter();
    return cd;
  }

  @Override
  public synchronized void commit() throws CommitException {
    SyncCommitObserver sco = null;
    try {
      sco = new SyncCommitObserver();
      commitAsync(sco);
      sco.waitForCommit();
    } finally {
      updates.clear();
      weakNotification = null;
      columnsRead.clear();
    }
  }

  void deleteWeakRow() {
    if (weakNotification != null) {
      env.getSharedResources().getBatchWriter()
          .writeMutation(weakNotification.newDelete(env, startTs));
    }
  }

  @Override
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

  // async experiment

  private abstract static class CommitCallback<V> implements FutureCallback<V> {

    private CommitData cd;

    CommitCallback(CommitData cd) {
      this.cd = cd;
    }

    @Override
    public void onSuccess(V result) {
      try {
        onSuccess(cd, result);
      } catch (Exception e) {
        cd.commitObserver.failed(e);
      }
    }

    protected abstract void onSuccess(CommitData cd, V result) throws Exception;


    @Override
    public void onFailure(Throwable t) {
      cd.commitObserver.failed(t);
    }

  }

  private abstract static class SynchronousCommitTask implements Runnable {

    private CommitData cd;

    SynchronousCommitTask(CommitData cd) {
      this.cd = cd;
    }

    protected abstract void runCommitStep(CommitData cd) throws Exception;

    @Override
    public void run() {
      try {
        runCommitStep(cd);
      } catch (Exception e) {
        cd.commitObserver.failed(e);
      }
    }
  }

  @Override
  public int getSize() {
    // TODO could calculate as items are added/set
    int size = 0;

    for (Entry<Bytes, Map<Column, Bytes>> entry : updates.entrySet()) {
      size += entry.getKey().length();
      for (Entry<Column, Bytes> entry2 : entry.getValue().entrySet()) {
        Column c = entry2.getKey();
        size += c.getFamily().length();
        size += c.getQualifier().length();
        size += c.getVisibility().length();
        size += entry2.getValue().length();
      }
    }

    for (Entry<Bytes, Set<Column>> entry : columnsRead.entrySet()) {
      size += entry.getKey().length();
      for (Column c : entry.getValue()) {
        size += c.getFamily().length();
        size += c.getQualifier().length();
        size += c.getVisibility().length();
      }
    }

    return size;
  }

  @Override
  public synchronized void commitAsync(AsyncCommitObserver commitCallback) {

    checkIfOpen();
    status = TxStatus.COMMIT_STARTED;
    commitAttempted = true;

    try {
      CommitData cd = createCommitData();
      beginCommitAsync(cd, commitCallback, null);
    } catch (Exception e) {
      e.printStackTrace();
      commitCallback.failed(e);
    }
  }

  private void beginCommitAsync(CommitData cd, AsyncCommitObserver commitCallback, RowColumn primary) {

    if (updates.size() == 0) {
      // TODO do async
      deleteWeakRow();
      commitCallback.committed();
      return;
    }

    for (Map<Column, Bytes> cols : updates.values()) {
      stats.incrementEntriesSet(cols.size());
    }

    Bytes primRow;
    Column primCol;

    if (primary != null) {
      primRow = primary.getRow();
      primCol = primary.getColumn();
      if (notification != null && !primary.equals(notification.getRowColumn())) {
        throw new IllegalArgumentException("Primary must be notification");
      }
    } else if (notification != null) {
      primRow = notification.getRow();
      primCol = notification.getColumn();
    } else {
      primRow = updates.keySet().iterator().next();
      Map<Column, Bytes> colSet = updates.get(primRow);
      primCol = colSet.keySet().iterator().next();
    }

    // get a primary column
    cd.prow = primRow;
    Map<Column, Bytes> colSet = updates.get(cd.prow);
    cd.pcol = primCol;
    cd.pval = colSet.remove(primCol);
    if (colSet.size() == 0) {
      updates.remove(cd.prow);
    }

    cd.commitObserver = commitCallback;

    // try to lock primary column
    final ConditionalMutation pcm =
        prewrite(cd.prow, cd.pcol, cd.pval, cd.prow, cd.pcol, isTriggerRow(cd.prow));


    ListenableFuture<Iterator<Result>> future = cd.acw.apply(Collections.singletonList(pcm));
    Futures.addCallback(future, new CommitCallback<Iterator<Result>>(cd) {
      @Override
      protected void onSuccess(CommitData cd, Iterator<Result> result) throws Exception {
        postLockPrimary(cd, pcm, Iterators.getOnlyElement(result));
      }
    }, env.getSharedResources().getAsyncCommitExecutor());
  }

  private void postLockPrimary(final CommitData cd, final ConditionalMutation pcm, Result result)
      throws Exception {
    final Status mutationStatus = result.getStatus();

    if (mutationStatus == Status.ACCEPTED) {
      lockOtherColumns(cd);
    } else {
      env.getSharedResources().getSyncCommitExecutor().execute(new SynchronousCommitTask(cd) {
        @Override
        protected void runCommitStep(CommitData cd) throws Exception {
          synchronousPostLockPrimary(cd, pcm, mutationStatus);
        }
      });
    }
  }

  private void synchronousPostLockPrimary(CommitData cd, ConditionalMutation pcm,
      Status mutationStatus) throws AccumuloException, AccumuloSecurityException, Exception {
    // TODO convert this code to async
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
          // TODO async
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
      getStats().setRejected(cd.getRejected());
      // TODO do async
      readUnread(cd);
      if (checkForAckCollision(pcm)) {
        cd.commitObserver.alreadyAcknowledged();
      } else {
        cd.commitObserver.commitFailed(cd.getShortCollisionMessage());
      }
      return;
    }

    lockOtherColumns(cd);
  }

  private void lockOtherColumns(CommitData cd) {
    ArrayList<ConditionalMutation> mutations = new ArrayList<>();

    for (Entry<Bytes, Map<Column, Bytes>> rowUpdates : updates.entrySet()) {
      ConditionalFlutation cm = null;

      for (Entry<Column, Bytes> colUpdates : rowUpdates.getValue().entrySet()) {
        if (cm == null) {
          cm =
              prewrite(rowUpdates.getKey(), colUpdates.getKey(), colUpdates.getValue(), cd.prow,
                  cd.pcol, false);
        } else {
          prewrite(cm, colUpdates.getKey(), colUpdates.getValue(), cd.prow, cd.pcol, false);
        }
      }

      mutations.add(cm);
    }

    cd.acceptedRows = new HashSet<>();


    ListenableFuture<Iterator<Result>> future = cd.bacw.apply(mutations);
    Futures.addCallback(future, new CommitCallback<Iterator<Result>>(cd) {
      @Override
      protected void onSuccess(CommitData cd, Iterator<Result> results) throws Exception {
        postLockOther(cd, results);
      }
    }, env.getSharedResources().getAsyncCommitExecutor());
  }

  private void postLockOther(final CommitData cd, Iterator<Result> results) throws Exception {
    while (results.hasNext()) {
      Result result = results.next();
      // TODO handle unknown?
      Bytes row = Bytes.of(result.getMutation().getRow());
      if (result.getStatus() == Status.ACCEPTED) {
        cd.acceptedRows.add(row);
      } else {
        cd.addToRejected(row, updates.get(row).keySet());
      }
    }

    if (cd.getRejected().size() > 0) {
      getStats().setRejected(cd.getRejected());
      env.getSharedResources().getSyncCommitExecutor().execute(new SynchronousCommitTask(cd) {
        @Override
        protected void runCommitStep(CommitData cd) throws Exception {
          readUnread(cd);
          rollbackOtherLocks(cd);
        }
      });
    } else if (stopAfterPreCommit) {
      cd.commitObserver.committed();
    } else {
      ListenableFuture<Stamp> future = env.getSharedResources().getOracleClient().getStampAsync();
      Futures.addCallback(future, new CommitCallback<Stamp>(cd) {
        @Override
        protected void onSuccess(CommitData cd, Stamp stamp) throws Exception {
          beginSecondCommitPhase(cd, stamp);
        }
      }, env.getSharedResources().getAsyncCommitExecutor());
    }
  }


  private void rollbackOtherLocks(CommitData cd) throws Exception {
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

    ListenableFuture<Void> future =
        env.getSharedResources().getBatchWriter().writeMutationsAsyncFuture(mutations);
    Futures.addCallback(future, new CommitCallback<Void>(cd) {
      @Override
      protected void onSuccess(CommitData cd, Void v) throws Exception {
        rollbackPrimaryLock(cd);
      }
    }, env.getSharedResources().getAsyncCommitExecutor());
  }

  private void rollbackPrimaryLock(CommitData cd) throws Exception {

    // mark transaction as complete for garbage collection purposes
    Flutation m = new Flutation(env, cd.prow);

    m.put(cd.pcol, ColumnConstants.DEL_LOCK_PREFIX | startTs,
        DelLockValue.encodeRollback(startTs, true, true));
    m.put(cd.pcol, ColumnConstants.TX_DONE_PREFIX | startTs, EMPTY);

    ListenableFuture<Void> future =
        env.getSharedResources().getBatchWriter().writeMutationsAsyncFuture(m);
    Futures.addCallback(future, new CommitCallback<Void>(cd) {
      @Override
      protected void onSuccess(CommitData cd, Void v) throws Exception {
        cd.commitObserver.commitFailed(cd.getShortCollisionMessage());
      }
    }, env.getSharedResources().getAsyncCommitExecutor());
  }


  private void beginSecondCommitPhase(CommitData cd, Stamp commitStamp) throws Exception {
    if (startTs < commitStamp.getGcTimestamp()) {
      rollbackOtherLocks(cd);
    } else {
      // Notification are written here for the following reasons :
      // * At this point all columns are locked, this guarantees that anything triggering as a
      // result of this transaction will see all of this transactions changes.
      // * The transaction is not yet committed. If the process dies at this point whatever
      // was running this transaction should rerun and recreate all of the notifications.
      // The next transactions will rerun because this transaction will have to be rolled back.
      // * If notifications are written in the 2nd phase of commit, then when the 2nd phase
      // partially succeeds notifications may never be written. Because in the case of failure
      // notifications would not be written until a column is read and it may never be read.
      // See https://github.com/fluo-io/fluo/issues/642
      //
      // Its very important the notifications which trigger an observer are deleted after the 2nd
      // phase of commit finishes.
      getStats().setCommitTs(commitStamp.getTxTimestamp());
      writeNotificationsAsync(cd, commitStamp.getTxTimestamp());
    }
  }

  private void writeNotificationsAsync(CommitData cd, final long commitTs) {

    HashMap<Bytes, Mutation> mutations = new HashMap<>();

    if (observedColumns.contains(cd.pcol) && isWrite(cd.pval) && !isDelete(cd.pval)) {
      Flutation m = new Flutation(env, cd.prow);
      Notification.put(env, m, cd.pcol, commitTs);
      mutations.put(cd.prow, m);
    }

    for (Entry<Bytes, Map<Column, Bytes>> rowUpdates : updates.entrySet()) {

      for (Entry<Column, Bytes> colUpdates : rowUpdates.getValue().entrySet()) {
        if (observedColumns.contains(colUpdates.getKey())) {
          Bytes val = colUpdates.getValue();
          if (isWrite(val) && !isDelete(val)) {
            Mutation m = mutations.get(rowUpdates.getKey());
            if (m == null) {
              m = new Flutation(env, rowUpdates.getKey());
              mutations.put(rowUpdates.getKey(), m);
            }
            Notification.put(env, m, colUpdates.getKey(), commitTs);
          }
        }
      }
    }

    for (Entry<Bytes, Set<Column>> entry : weakNotifications.entrySet()) {
      Mutation m = mutations.get(entry.getKey());
      if (m == null) {
        m = new Flutation(env, entry.getKey());
        mutations.put(entry.getKey(), m);
      }
      for (Column col : entry.getValue()) {
        Notification.put(env, m, col, commitTs);
      }
    }


    ListenableFuture<Void> future =
        env.getSharedResources().getBatchWriter().writeMutationsAsyncFuture(mutations.values());
    Futures.addCallback(future, new CommitCallback<Void>(cd) {
      @Override
      protected void onSuccess(CommitData cd, Void v) throws Exception {
        commmitPrimary(cd, commitTs);
      }
    }, env.getSharedResources().getAsyncCommitExecutor());
  }

  private void commmitPrimary(CommitData cd, final long commitTs) {
    // try to delete lock and add write for primary column
    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    boolean isTrigger = isTriggerRow(cd.prow) && cd.pcol.equals(notification.getColumn());

    Condition lockCheck =
        new FluoCondition(env, cd.pcol).setIterators(iterConf).setValue(
            LockValue.encode(cd.prow, cd.pcol, isWrite(cd.pval), isDelete(cd.pval), isTrigger,
                getTransactorID()));
    final ConditionalMutation delLockMutation = new ConditionalFlutation(env, cd.prow, lockCheck);

    ColumnUtil.commitColumn(env, isTrigger, true, cd.pcol, isWrite(cd.pval), isDelete(cd.pval),
        startTs, commitTs, observedColumns, delLockMutation);


    ListenableFuture<Iterator<Result>> future =
        cd.acw.apply(Collections.singletonList(delLockMutation));
    Futures.addCallback(future, new CommitCallback<Iterator<Result>>(cd) {
      @Override
      protected void onSuccess(CommitData cd, Iterator<Result> result) throws Exception {
        handleUnkownStatsAfterPrimary(cd, commitTs, delLockMutation,
            Iterators.getOnlyElement(result));
      }
    }, env.getSharedResources().getAsyncCommitExecutor());
  }

  private void handleUnkownStatsAfterPrimary(CommitData cd, final long commitTs,
      final ConditionalMutation delLockMutation, Result result) throws Exception {

    final Status mutationStatus = result.getStatus();
    if (mutationStatus == Status.UNKNOWN) {
      // the code for handing this is synchronous and needs to be handled in another thread pool
      Runnable task = new SynchronousCommitTask(cd) {
        @Override
        protected void runCommitStep(CommitData cd) throws Exception {

          Status ms = mutationStatus;

          while (ms == Status.UNKNOWN) {

            // TODO async
            TxInfo txInfo = TxInfo.getTransactionInfo(env, cd.prow, cd.pcol, startTs);

            switch (txInfo.status) {
              case COMMITTED:
                if (txInfo.commitTs != commitTs) {
                  throw new IllegalStateException(cd.prow + " " + cd.pcol + " " + txInfo.commitTs
                      + "!=" + commitTs);
                }
                ms = Status.ACCEPTED;
                break;
              case LOCKED:
                // TODO async
                ms = cd.cw.write(delLockMutation).getStatus();
                break;
              default:
                ms = Status.REJECTED;
            }
          }

          postCommitPrimary(cd, commitTs, ms);
        }
      };

      env.getSharedResources().getSyncCommitExecutor().execute(task);
    } else {
      postCommitPrimary(cd, commitTs, mutationStatus);
    }
  }


  private void postCommitPrimary(CommitData cd, long commitTs, Status mutationStatus)
      throws Exception {
    if (mutationStatus != Status.ACCEPTED) {
      cd.commitObserver.commitFailed(cd.getShortCollisionMessage());
    } else {
      if (stopAfterPrimaryCommit) {
        cd.commitObserver.committed();
      } else {
        deleteLocks(cd, commitTs);
      }
    }
  }

  private void deleteLocks(CommitData cd, final long commitTs) {
    // delete locks and add writes for other columns
    ArrayList<Mutation> mutations = new ArrayList<>(updates.size() + 1);
    for (Entry<Bytes, Map<Column, Bytes>> rowUpdates : updates.entrySet()) {
      Flutation m = new Flutation(env, rowUpdates.getKey());
      boolean isTriggerRow = isTriggerRow(rowUpdates.getKey());
      for (Entry<Column, Bytes> colUpdates : rowUpdates.getValue().entrySet()) {
        ColumnUtil.commitColumn(env,
            isTriggerRow && colUpdates.getKey().equals(notification.getColumn()), false,
            colUpdates.getKey(), isWrite(colUpdates.getValue()), isDelete(colUpdates.getValue()),
            startTs, commitTs, observedColumns, m);
      }

      mutations.add(m);
    }


    ListenableFuture<Void> future =
        env.getSharedResources().getBatchWriter().writeMutationsAsyncFuture(mutations);
    Futures.addCallback(future, new CommitCallback<Void>(cd) {
      @Override
      protected void onSuccess(CommitData cd, Void v) throws Exception {
        finishCommit(cd, commitTs);
      }
    }, env.getSharedResources().getAsyncCommitExecutor());

  }

  @VisibleForTesting
  public boolean finishCommit(CommitData cd, Stamp commitStamp) throws TableNotFoundException,
      MutationsRejectedException {
    deleteLocks(cd, commitStamp.getTxTimestamp());
    return true;
  }

  private void finishCommit(CommitData cd, long commitTs) {
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

    env.getSharedResources().getBatchWriter().writeMutationsAsync(afterFlushMutations);

    cd.commitObserver.committed();
  }

  public SnapshotScanner newSnapshotScanner(Span span, Collection<Column> columns) {
    return new SnapshotScanner(env, new SnapshotScanner.Opts(span, columns), startTs, stats);
  }
}
