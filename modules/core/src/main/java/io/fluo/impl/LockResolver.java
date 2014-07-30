package io.fluo.impl;

import io.fluo.api.Column;
import io.fluo.impl.iterators.PrewriteIterator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.ConditionalWriter.Status;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Condition;
import org.apache.accumulo.core.data.ConditionalMutation;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * This is utility code for either rolling forward or back failed transactions. A transaction is deemed to have failed if the reading transaction waited too
 * long or the transactor id does not exist in zookeeper.
 */

public class LockResolver {

  private static Map<PrimaryRowColumn,List<Entry<Key,Value>>> groupLocksByPrimary(List<Entry<Key,Value>> locks) {
    Map<PrimaryRowColumn,List<Entry<Key,Value>>> groupedLocks = new HashMap<>();
    Map<PrimaryRowColumn,Long> transactorIds = new HashMap<>();

    for (Entry<Key,Value> lock : locks) {
      LockValue lockVal = new LockValue(lock.getValue().get());
      PrimaryRowColumn prc = new PrimaryRowColumn(lockVal.getPrimaryRow(), lockVal.getPrimaryColumn(), lock.getKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK);

      List<Entry<Key,Value>> lockList = groupedLocks.get(prc);
      if (lockList == null) {
        lockList = new ArrayList<>();
        groupedLocks.put(prc, lockList);
      }

      Long trid = transactorIds.get(prc);
      if (trid == null) {
        transactorIds.put(prc, lockVal.getTransactor());
      } else if (trid != lockVal.getTransactor()) {
        // sanity check.. its assumed that all locks w/ the same PrimaryRowColumn should have the same transactor id as well
        throw new IllegalStateException("transactor ids not equals " + prc + " " + lock.getKey() + " " + trid + " " + lockVal.getTransactor());
      }

      lockList.add(lock);
    }

    return groupedLocks;

  }

  /**
   * Attempts to roll forward or roll back a set of locks encountered by a transaction reading data.
   * 
   * @param aconfig
   *          environment
   * @param startTs
   *          the logical start time from the oracle of the transaction that encountered the lock
   * @param stats
   *          stats object for the transaction that encountered the lock
   * @param locks
   * @param startTime
   *          the wall time that the transaction that encountered the lock first saw the lock
   * @return true if all locks passed in were resolved (rolled forward or back)
   */

  static boolean resolveLocks(Configuration aconfig, long startTs, TxStats stats, List<Entry<Key,Value>> locks, long startTime) {
    // check if transactor is still alive

    int numResolved = 0;

    Map<ByteSequence,Mutation> mutations = new HashMap<>();

    boolean timedOut = false;

    TransactorCache transactorCache = aconfig.getSharedResources().getTransactorCache();

    List<Entry<Key,Value>> locksToRecover;
    if (System.currentTimeMillis() - startTime > aconfig.getRollbackTime()) {
      locksToRecover = locks;
      stats.incrementTimedOutLocks(locksToRecover.size());
      timedOut = true;
    } else {
      locksToRecover = new ArrayList<>(locks.size());
      for (Entry<Key,Value> entry : locks) {

        Long transactorId = new LockValue(entry.getValue().get()).getTransactor();
        long lockTs = entry.getKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;

        if (transactorCache.checkTimedout(transactorId, lockTs)) {
          locksToRecover.add(entry);
          stats.incrementTimedOutLocks();
        } else if (!transactorCache.checkExists(transactorId)) {
          locksToRecover.add(entry);
          stats.incrementDeadLocks();
        }
      }
    }

    Map<PrimaryRowColumn,List<Entry<Key,Value>>> groupedLocks = groupLocksByPrimary(locksToRecover);

    if (timedOut) {
      Set<Entry<PrimaryRowColumn,List<Entry<Key,Value>>>> es = groupedLocks.entrySet();

      for (Entry<PrimaryRowColumn,List<Entry<Key,Value>>> entry : es) {
        long lockTs = entry.getKey().startTs;
        Long transactorId = new LockValue(entry.getValue().get(0).getValue().get()).getTransactor();
        transactorCache.addTimedoutTransactor(transactorId, lockTs, startTime);
      }
    }

    TxInfoCache txiCache = aconfig.getSharedResources().getTxInfoCache();

    Set<Entry<PrimaryRowColumn,List<Entry<Key,Value>>>> es = groupedLocks.entrySet();
    for (Entry<PrimaryRowColumn,List<Entry<Key,Value>>> group : es) {
      TxInfo txInfo = txiCache.getTransactionInfo(group.getKey());
      switch (txInfo.status) {
        case COMMITTED:
          commitColumns(aconfig, group.getKey(), group.getValue(), txInfo.commitTs, mutations);
          numResolved += group.getValue().size();
          break;
        case LOCKED:
          if (rollbackPrimary(aconfig, startTs, group.getKey(), txInfo.lockValue)) {
            rollback(aconfig, startTs, group.getKey(), group.getValue(), mutations);
            numResolved += group.getValue().size();
          }
          break;
        case ROLLED_BACK:
          // TODO ensure this if ok if there concurrent rollback
          rollback(aconfig, startTs, group.getKey(), group.getValue(), mutations);
          numResolved += group.getValue().size();
          break;
        case UNKNOWN:
        default:
          throw new IllegalStateException("can not abort : " + group.getKey() + " (" + txInfo.status + ")");
      }
    }

    if (mutations.size() > 0)
      aconfig.getSharedResources().getBatchWriter().writeMutations(new ArrayList<>(mutations.values()));

    return numResolved == locks.size();

  }

  private static void rollback(Configuration aconfig, long startTs, PrimaryRowColumn prc, List<Entry<Key,Value>> value, Map<ByteSequence,Mutation> mutations) {
    for (Entry<Key,Value> entry : value) {
      if (isPrimary(prc, entry.getKey()))
        continue;

      long lockTs = entry.getKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
      Mutation mut = getMutation(entry.getKey().getRowData(), mutations);
      Key k = entry.getKey();
      mut.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(), k.getColumnVisibilityParsed(), ColumnUtil.DEL_LOCK_PREFIX | startTs,
          DelLockValue.encode(lockTs, false, true));
    }

  }

  private static boolean rollbackPrimary(Configuration aconfig, long startTs, PrimaryRowColumn prc, byte[] lockValue) {
    // TODO use cached CV
    ColumnVisibility cv = prc.pcol.getVisibilityParsed();

    // TODO review use of PrewriteIter here

    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    // TODO cache col vis?
    ConditionalMutation delLockMutation = new ConditionalMutation(ByteUtil.toByteSequence(prc.prow), new Condition(
        ByteUtil.toByteSequence(prc.pcol.getFamily()), ByteUtil.toByteSequence(prc.pcol.getQualifier())).setIterators(iterConf).setVisibility(cv)
        .setValue(lockValue));

    // TODO sanity check on lockTs vs startTs

    delLockMutation.put(prc.pcol.getFamily().toArray(), prc.pcol.getQualifier().toArray(), cv, ColumnUtil.DEL_LOCK_PREFIX | startTs,
        DelLockValue.encode(prc.startTs, true, true));

    ConditionalWriter cw = null;

    cw = aconfig.getSharedResources().getConditionalWriter();

    // TODO handle other conditional writer cases
    try {
      return cw.write(delLockMutation).getStatus() == Status.ACCEPTED;
    } catch (AccumuloException e) {
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private static void commitColumns(Configuration aconfig, PrimaryRowColumn prc, List<Entry<Key,Value>> value, long commitTs,
      Map<ByteSequence,Mutation> mutations) {
    for (Entry<Key,Value> entry : value) {
      if (isPrimary(prc, entry.getKey()))
        continue;

      long lockTs = entry.getKey().getTimestamp() & ColumnUtil.TIMESTAMP_MASK;
      // TODO may be that a stronger sanity check that could be done here
      if (commitTs < lockTs) {
        throw new IllegalStateException("bad commitTs : " + entry.getKey() + " (" + commitTs + "<" + lockTs + ")");
      }

      Mutation mut = getMutation(entry.getKey().getRowData(), mutations);
      Column col = new Column(new ArrayBytes(entry.getKey().getColumnFamilyData()), new ArrayBytes(entry.getKey().getColumnQualifierData()))
          .setVisibility(entry.getKey().getColumnVisibilityParsed());
      LockValue lv = new LockValue(entry.getValue().get());
      ColumnUtil.commitColumn(lv.isTrigger(), false, col, lv.isWrite(), lv.isDelete(), lockTs, commitTs, aconfig.getObservers().keySet(), mut);
    }

  }

  private static Mutation getMutation(ByteSequence row, Map<ByteSequence,Mutation> mutations) {
    Mutation mut = mutations.get(row);

    if (mut == null) {
      mut = new Mutation(row.toArray());
      mutations.put(row, mut);
    }

    return mut;
  }

  private static boolean isPrimary(PrimaryRowColumn prc, Key k) {
    return prc.prow.equals(k.getRowData())
        && prc.pcol.equals(new Column(new ArrayBytes(k.getColumnFamilyData()), new ArrayBytes(k.getColumnQualifierData())).setVisibility(k
            .getColumnVisibilityParsed()));
  }
}
