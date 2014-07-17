package io.fluo.impl;

import io.fluo.api.Column;
import io.fluo.api.exceptions.AlreadyAcknowledgedException;
import io.fluo.api.exceptions.CommitException;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedTransaction;
import io.fluo.impl.TransactionImpl.CommitData;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ArrayByteSequence;

public class TestTransaction extends TypedTransaction {

  private TransactionImpl tx;

  public TestTransaction(Configuration config) throws Exception {
    this(new TransactionImpl(config), new StringEncoder());
  }

  public TestTransaction(TransactionImpl transactionImpl, StringEncoder stringEncoder) {
    super(transactionImpl, stringEncoder, new TypeLayer(stringEncoder));
    this.tx = transactionImpl;
  }

  public TestTransaction(Configuration config, ArrayByteSequence trow, Column tcol) throws Exception {
    this(new TransactionImpl(config, trow, tcol), new StringEncoder());
  }

  public TestTransaction(Configuration config, String trow, Column tcol) throws Exception {
    this(new TransactionImpl(config, new ArrayByteSequence(trow), tcol), new StringEncoder());
  }

  public void commit() throws CommitException {
    tx.commit();
  }

  public CommitData createCommitData() throws TableNotFoundException {
    return tx.createCommitData();
  }

  public boolean preCommit(CommitData cd) throws AlreadyAcknowledgedException, TableNotFoundException, AccumuloException, AccumuloSecurityException {
    return tx.preCommit(cd);
  }

  public boolean preCommit(CommitData cd, ArrayByteSequence trow, Column tcol) throws AlreadyAcknowledgedException, TableNotFoundException, AccumuloException,
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

}
