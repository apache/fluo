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

package org.apache.fluo.integration;

import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.fluo.accumulo.iterators.NotificationIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.util.NotificationUtil;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;
import org.apache.fluo.api.types.TypedTransactionBase;
import org.apache.fluo.core.exceptions.AlreadyAcknowledgedException;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.impl.TransactionImpl;
import org.apache.fluo.core.impl.TransactionImpl.CommitData;
import org.apache.fluo.core.impl.TransactorNode;
import org.apache.fluo.core.impl.TxStats;
import org.apache.fluo.core.oracle.Stamp;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.SpanUtil;
import org.apache.hadoop.io.Text;

public class TestTransaction extends TypedTransactionBase implements TransactionBase {

  private TransactionImpl tx;
  private Environment env;

  public static long getNotificationTS(Environment env, String row, Column col) {
    Scanner scanner;
    try {
      scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
    IteratorSetting iterCfg = new IteratorSetting(11, NotificationIterator.class);
    scanner.addScanIterator(iterCfg);

    Text cv = ByteUtil.toText(col.getVisibility());

    scanner.setRange(SpanUtil.toRange(Span.prefix(row)));
    scanner.fetchColumn(ByteUtil.toText(ColumnConstants.NOTIFY_CF),
        new Text(NotificationUtil.encodeCol(col)));

    for (Entry<Key, org.apache.accumulo.core.data.Value> entry : scanner) {
      if (entry.getKey().getColumnVisibility().equals(cv)) {
        return Notification.from(entry.getKey()).getTimestamp();
      }
    }

    throw new RuntimeException("No notification found");
  }

  @SuppressWarnings("resource")
  public TestTransaction(Environment env, TransactorNode transactor) {
    this(new TransactionImpl(env).setTransactor(transactor), new StringEncoder(), env);
  }

  public TestTransaction(Environment env) {
    this(new TransactionImpl(env), new StringEncoder(), env);
  }

  private TestTransaction(TransactionImpl transactionImpl, StringEncoder stringEncoder,
      Environment env) {
    super(transactionImpl, stringEncoder, new TypeLayer(stringEncoder));
    this.tx = transactionImpl;
    this.env = env;
  }

  public TestTransaction(Environment env, String trow, Column tcol) {
    this(env, trow, tcol, getNotificationTS(env, trow, tcol));
  }

  public TestTransaction(Environment env, String trow, Column tcol, long notificationTS) {
    this(new TransactionImpl(env, new Notification(Bytes.of(trow), tcol, notificationTS)),
        new StringEncoder(), env);
  }

  /**
   * Calls commit() and then close()
   */
  public void done() throws CommitException {
    try {
      commit();
    } finally {
      close();
    }
  }

  public void commit() throws CommitException {
    tx.commit();
    env.getSharedResources().getBatchWriter().waitForAsyncFlush();
  }

  public void close() {
    tx.close();
  }

  public CommitData createCommitData() throws TableNotFoundException {
    return tx.createCommitData();
  }

  public boolean preCommit(CommitData cd) throws AlreadyAcknowledgedException,
      TableNotFoundException, AccumuloException, AccumuloSecurityException {
    return tx.preCommit(cd);
  }

  public boolean preCommit(CommitData cd, RowColumn primary) {
    return tx.preCommit(cd, primary);
  }

  public boolean commitPrimaryColumn(CommitData cd, Stamp commitStamp) throws AccumuloException,
      AccumuloSecurityException {
    return tx.commitPrimaryColumn(cd, commitStamp);
  }

  public void finishCommit(CommitData cd, Stamp commitStamp) throws MutationsRejectedException,
      TableNotFoundException {
    tx.finishCommit(cd, commitStamp);
    env.getSharedResources().getBatchWriter().waitForAsyncFlush();
  }

  public long getStartTs() {
    return tx.getStartTs();
  }

  public TxStats getStats() {
    return tx.getStats();
  }
}
