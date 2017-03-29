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
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.accumulo.iterators.PrewriteIterator;
import org.apache.fluo.accumulo.util.ColumnConstants;
import org.apache.fluo.accumulo.values.DelLockValue;
import org.apache.fluo.accumulo.values.LockValue;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.ColumnUtil;
import org.apache.fluo.core.util.ConditionalFlutation;
import org.apache.fluo.core.util.FluoCondition;
import org.apache.fluo.core.util.SpanUtil;

import static org.apache.fluo.api.observer.Observer.NotificationType.STRONG;

/**
 * This is utility code for either rolling forward or back failed transactions. A transaction is
 * deemed to have failed if the reading transaction waited too long or the transactor id does not
 * exist in zookeeper.
 */

public class LockResolver {

  private static Map<PrimaryRowColumn, List<Entry<Key, Value>>> groupLocksByPrimary(
      List<Entry<Key, Value>> locks) {
    Map<PrimaryRowColumn, List<Entry<Key, Value>>> groupedLocks = new HashMap<>();
    Map<PrimaryRowColumn, Long> transactorIds = new HashMap<>();

    for (Entry<Key, Value> lock : locks) {
      LockValue lockVal = new LockValue(lock.getValue().get());
      PrimaryRowColumn prc =
          new PrimaryRowColumn(lockVal.getPrimaryRow(), lockVal.getPrimaryColumn(), lock.getKey()
              .getTimestamp() & ColumnConstants.TIMESTAMP_MASK);

      List<Entry<Key, Value>> lockList = groupedLocks.get(prc);
      if (lockList == null) {
        lockList = new ArrayList<>();
        groupedLocks.put(prc, lockList);
      }

      Long trid = transactorIds.get(prc);
      if (trid == null) {
        transactorIds.put(prc, lockVal.getTransactor());
      } else if (!trid.equals(lockVal.getTransactor())) {
        // sanity check.. its assumed that all locks w/ the same PrimaryRowColumn should have the
        // same transactor id as well
        throw new IllegalStateException("transactor ids not equals " + prc + " " + lock.getKey()
            + " " + trid + " " + lockVal.getTransactor());
      }

      lockList.add(lock);
    }

    return groupedLocks;

  }

  /**
   * Attempts to roll forward or roll back a set of locks encountered by a transaction reading data.
   *
   * @param env environment
   * @param startTs The logical start time from the oracle of the transaction that encountered the
   *        lock
   * @param stats Stats object for the transaction that encountered the lock
   * @param locks List of locks
   * @param startTime The wall time that the transaction that encountered the lock first saw the
   *        lock
   * @return true if all locks passed in were resolved (rolled forward or back)
   */
  static boolean resolveLocks(Environment env, long startTs, TxStats stats,
      List<Entry<Key, Value>> locks, long startTime) {
    // check if transactor is still alive

    int numResolved = 0;

    Map<ByteSequence, Mutation> mutations = new HashMap<>();

    boolean timedOut = false;

    TransactorCache transactorCache = env.getSharedResources().getTransactorCache();

    List<Entry<Key, Value>> locksToRecover;
    if (System.currentTimeMillis() - startTime > env.getConfiguration()
        .getTransactionRollbackTime()) {
      locksToRecover = locks;
      stats.incrementTimedOutLocks(locksToRecover.size());
      timedOut = true;
    } else {
      locksToRecover = new ArrayList<>(locks.size());
      for (Entry<Key, Value> entry : locks) {

        Long transactorId = new LockValue(entry.getValue().get()).getTransactor();
        long lockTs = entry.getKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;

        if (transactorCache.checkTimedout(transactorId, lockTs)) {
          locksToRecover.add(entry);
          stats.incrementTimedOutLocks();
        } else if (!transactorCache.checkExists(transactorId)) {
          locksToRecover.add(entry);
          stats.incrementDeadLocks();
        }
      }
    }

    Map<PrimaryRowColumn, List<Entry<Key, Value>>> groupedLocks =
        groupLocksByPrimary(locksToRecover);

    if (timedOut) {
      Set<Entry<PrimaryRowColumn, List<Entry<Key, Value>>>> es = groupedLocks.entrySet();

      for (Entry<PrimaryRowColumn, List<Entry<Key, Value>>> entry : es) {
        long lockTs = entry.getKey().startTs;
        Long transactorId = new LockValue(entry.getValue().get(0).getValue().get()).getTransactor();
        transactorCache.addTimedoutTransactor(transactorId, lockTs, startTime);
      }
    }

    TxInfoCache txiCache = env.getSharedResources().getTxInfoCache();

    Set<Entry<PrimaryRowColumn, List<Entry<Key, Value>>>> es = groupedLocks.entrySet();
    for (Entry<PrimaryRowColumn, List<Entry<Key, Value>>> group : es) {
      TxInfo txInfo = txiCache.getTransactionInfo(group.getKey());
      switch (txInfo.status) {
        case COMMITTED:
          commitColumns(env, group.getKey(), group.getValue(), txInfo.commitTs, mutations);
          numResolved += group.getValue().size();
          break;
        case LOCKED:
          if (rollbackPrimary(env, startTs, group.getKey(), txInfo.lockValue)) {
            rollback(env, startTs, group.getKey(), group.getValue(), mutations);
            numResolved += group.getValue().size();
          }
          break;
        case ROLLED_BACK:
          // TODO ensure this if ok if there concurrent rollback
          rollback(env, startTs, group.getKey(), group.getValue(), mutations);
          numResolved += group.getValue().size();
          break;
        case UNKNOWN:
        default:
          throw new IllegalStateException("can not abort : " + group.getKey() + " ("
              + txInfo.status + ")");
      }
    }

    if (mutations.size() > 0) {
      env.getSharedResources().getBatchWriter().writeMutations(new ArrayList<>(mutations.values()));
    }

    return numResolved == locks.size();
  }

  private static void rollback(Environment env, long startTs, PrimaryRowColumn prc,
      List<Entry<Key, Value>> value, Map<ByteSequence, Mutation> mutations) {
    for (Entry<Key, Value> entry : value) {
      if (isPrimary(prc, entry.getKey())) {
        continue;
      }

      long lockTs = entry.getKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;
      Mutation mut = getMutation(entry.getKey().getRowData(), mutations);
      Key k = entry.getKey();
      mut.put(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(),
          k.getColumnVisibilityParsed(), ColumnConstants.DEL_LOCK_PREFIX | lockTs,
          DelLockValue.encodeRollback(false, true));
    }

  }

  private static boolean rollbackPrimary(Environment env, long startTs, PrimaryRowColumn prc,
      byte[] lockValue) {
    // TODO review use of PrewriteIter here

    IteratorSetting iterConf = new IteratorSetting(10, PrewriteIterator.class);
    PrewriteIterator.setSnaptime(iterConf, startTs);
    ConditionalFlutation delLockMutation =
        new ConditionalFlutation(env, prc.prow, new FluoCondition(env, prc.pcol).setIterators(
            iterConf).setValue(lockValue));

    delLockMutation.put(prc.pcol, ColumnConstants.DEL_LOCK_PREFIX | prc.startTs,
        DelLockValue.encodeRollback(true, true));

    ConditionalWriter cw = null;

    cw = env.getSharedResources().getConditionalWriter();

    // TODO handle other conditional writer cases
    try {
      return cw.write(delLockMutation).getStatus() == Status.ACCEPTED;
    } catch (AccumuloException e) {
      throw new RuntimeException(e);
    } catch (AccumuloSecurityException e) {
      throw new RuntimeException(e);
    }
  }

  private static void commitColumns(Environment env, PrimaryRowColumn prc,
      List<Entry<Key, Value>> value, long commitTs, Map<ByteSequence, Mutation> mutations) {
    for (Entry<Key, Value> entry : value) {
      if (isPrimary(prc, entry.getKey())) {
        continue;
      }

      long lockTs = entry.getKey().getTimestamp() & ColumnConstants.TIMESTAMP_MASK;
      // TODO may be that a stronger sanity check that could be done here
      if (commitTs < lockTs) {
        throw new IllegalStateException("bad commitTs : " + entry.getKey() + " (" + commitTs + "<"
            + lockTs + ")");
      }

      Mutation mut = getMutation(entry.getKey().getRowData(), mutations);
      Column col = SpanUtil.toRowColumn(entry.getKey()).getColumn();

      LockValue lv = new LockValue(entry.getValue().get());
      ColumnUtil.commitColumn(env, lv.isTrigger(), false, col, lv.isWrite(), lv.isDelete(), lockTs,
          commitTs, env.getConfiguredObservers().getObservedColumns(STRONG), mut);
    }

  }

  private static Mutation getMutation(ByteSequence row, Map<ByteSequence, Mutation> mutations) {
    Mutation mut = mutations.get(row);

    if (mut == null) {
      mut = new Mutation(row.toArray());
      mutations.put(row, mut);
    }

    return mut;
  }

  private static boolean isPrimary(PrimaryRowColumn prc, Key k) {
    return prc.prow.equals(ByteUtil.toBytes(k.getRowData()))
        && prc.pcol.equals(SpanUtil.toRowColumn(k).getColumn());
  }
}
