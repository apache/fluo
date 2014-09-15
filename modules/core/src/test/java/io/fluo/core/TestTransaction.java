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
package io.fluo.core;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.exceptions.CommitException;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.core.exceptions.AlreadyAcknowledgedException;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.TransactionImpl;
import io.fluo.core.impl.TransactionImpl.CommitData;
import io.fluo.core.impl.TransactorNode;
import io.fluo.core.impl.TxStats;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;

public class TestTransaction extends TypedTransactionBase implements TransactionBase {

  private TransactionImpl tx;
  
  @SuppressWarnings("resource")
  public TestTransaction(Environment env, TransactorNode transactor) {
    this(new TransactionImpl(env).setTransactor(transactor), new StringEncoder());
  }

  public TestTransaction(Environment env) {
    this(new TransactionImpl(env), new StringEncoder());
  }

  public TestTransaction(TransactionImpl transactionImpl, StringEncoder stringEncoder) {
    super(transactionImpl, stringEncoder, new TypeLayer(stringEncoder));
    this.tx = transactionImpl;
  }

  public TestTransaction(Environment env, Bytes trow, Column tcol) {
    this(new TransactionImpl(env, trow, tcol), new StringEncoder());
  }

  public TestTransaction(Environment env, String trow, Column tcol) {
    this(new TransactionImpl(env, Bytes.wrap(trow), tcol), new StringEncoder());
  }

  /**
   * Calls commit() and then close()
   * 
   * @throws CommitException
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
  }
  
  public void close() {
    tx.close();
  }
  
  public CommitData createCommitData() throws TableNotFoundException {
    return tx.createCommitData();
  }

  public boolean preCommit(CommitData cd) throws AlreadyAcknowledgedException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
    return tx.preCommit(cd);
  }

  public boolean preCommit(CommitData cd, Bytes trow, Column tcol) throws AlreadyAcknowledgedException, TableNotFoundException, AccumuloException,
      AccumuloSecurityException {
    return tx.preCommit(cd, trow, tcol);
  }

  public boolean commitPrimaryColumn(CommitData cd, long commitTs) throws AccumuloException, AccumuloSecurityException {
    return tx.commitPrimaryColumn(cd, commitTs);
  }

  public void finishCommit(CommitData cd, long commitTs) throws MutationsRejectedException, TableNotFoundException {
    tx.finishCommit(cd, commitTs);
  }

  public long getStartTs() {
    return tx.getStartTs();
  }
  
  public TxStats getStats() {
    return tx.getStats();
  }
}
