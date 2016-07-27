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

package org.apache.fluo.integration.impl;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.Stat;
import org.apache.fluo.accumulo.format.FluoFormatter;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.exceptions.CommitException;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test starts multiple thread that randomly transfer between accounts. At any given time the
 * sum of all money in the bank should be the same, therefore the average should not vary.
 */
public class StochasticBankIT extends ITBaseImpl {

  private static final Logger log = LoggerFactory.getLogger(StochasticBankIT.class);
  private static AtomicInteger txCount = new AtomicInteger();

  @Test
  public void testConcurrency() throws Exception {

    conn.tableOperations().setProperty(table, Property.TABLE_MAJC_RATIO.getKey(), "1");
    conn.tableOperations().setProperty(table, Property.TABLE_BLOCKCACHE_ENABLED.getKey(), "true");

    int numAccounts = 5000;

    TreeSet<Text> splits = new TreeSet<>();
    splits.add(new Text(fmtAcct(numAccounts / 4)));
    splits.add(new Text(fmtAcct(numAccounts / 2)));
    splits.add(new Text(fmtAcct(3 * numAccounts / 4)));

    conn.tableOperations().addSplits(table, splits);

    AtomicBoolean runFlag = new AtomicBoolean(true);

    populate(env, numAccounts);

    Random rand = new Random();
    Environment tenv = env;

    if (rand.nextBoolean()) {
      tenv = new FaultyConfig(env, (rand.nextDouble() * .4) + .1, .50);
    }

    List<Thread> threads = startTransfers(tenv, numAccounts, 20, runFlag);

    runVerifier(env, numAccounts, 100);

    runFlag.set(false);

    for (Thread thread : threads) {
      thread.join();
    }

    log.debug("txCount : " + txCount.get());
    Assert.assertTrue("txCount : " + txCount.get(), txCount.get() > 0);

    runVerifier(env, numAccounts, 1);
  }

  private static Column balanceCol = new Column("data", "balance");

  private static void populate(Environment env, int numAccounts) throws Exception {
    TestTransaction tx = new TestTransaction(env);

    for (int i = 0; i < numAccounts; i++) {
      tx.set(fmtAcct(i), balanceCol, "1000");
    }

    tx.done();
  }

  private static String fmtAcct(int i) {
    return String.format("%09d", i);
  }

  private static List<Thread> startTransfers(final Environment env, final int numAccounts,
      int numThreads, final AtomicBoolean runFlag) {

    ArrayList<Thread> threads = new ArrayList<>();

    for (int i = 0; i < numThreads; i++) {
      Runnable task = new Runnable() {
        Random rand = new Random();

        @Override
        public void run() {
          while (runFlag.get()) {
            int acct1 = rand.nextInt(numAccounts);
            int acct2 = rand.nextInt(numAccounts);
            while (acct1 == acct2) {
              acct2 = rand.nextInt(numAccounts);
            }
            int amt = rand.nextInt(100);

            transfer(env, fmtAcct(acct1), fmtAcct(acct2), amt);
          }

        }

      };
      Thread thread = new Thread(task);
      thread.start();
      threads.add(thread);
    }

    return threads;
  }

  private static void transfer(Environment env, String from, String to, int amt) {
    try {

      while (true) {
        try {
          TestTransaction tx = new TestTransaction(env);
          int bal1 = Integer.parseInt(tx.gets(from, balanceCol));
          int bal2 = Integer.parseInt(tx.gets(to, balanceCol));

          if (bal1 - amt >= 0) {
            tx.set(from, balanceCol, (bal1 - amt) + "");
            tx.set(to, balanceCol, (bal2 + amt) + "");
          } else {
            break;
          }

          tx.done();
          break;

        } catch (CommitException ce) {
          // retry
        }
      }

      txCount.incrementAndGet();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  private static void runVerifier(Environment env, int numAccounts, int num) {
    TestTransaction lastTx = null;

    try {

      for (int i = 0; i < num; i++) {

        if (i == num / 2) {
          env.getConnector().tableOperations().compact(env.getTable(), null, null, true, false);
        }

        long t1 = System.currentTimeMillis();

        TestTransaction tx = new TestTransaction(env);
        Stat stat = new Stat();

        for (RowColumnValue rcv : tx.scanner().build()) {
          int amt = Integer.parseInt(rcv.getValue().toString());
          stat.addStat(amt);
        }

        long t2 = System.currentTimeMillis();

        log.debug("avg : %,9.2f  min : %,6d  max : %,6d  stddev : %1.2f  rate : %,6.2f\n",
            stat.getAverage(), stat.getMin(), stat.getMax(), stat.getStdDev(), numAccounts
                / ((t2 - t1) / 1000.0));

        if (stat.getSum() != numAccounts * 1000) {
          if (lastTx != null) {
            printDiffs(env, lastTx, tx);
          }
        }

        Assert.assertEquals(numAccounts * 1000, stat.getSum());

        lastTx = tx;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void printDiffs(Environment env, TestTransaction lastTx, TestTransaction tx)
      throws Exception {
    Map<String, String> bals1 = toMap(lastTx);
    Map<String, String> bals2 = toMap(tx);

    if (!bals1.keySet().equals(bals2.keySet())) {
      log.debug("KS NOT EQ");
    }

    int sum1 = 0;
    int sum2 = 0;

    for (Entry<String, String> entry : bals1.entrySet()) {
      String val2 = bals2.get(entry.getKey());

      if (!entry.getValue().equals(val2)) {
        int v1 = Integer.parseInt(entry.getValue());
        int v2 = Integer.parseInt(val2);
        sum1 += v1;
        sum2 += v2;

        log.debug(entry.getKey() + " " + entry.getValue() + " " + val2 + " " + (v2 - v1));
      }
    }

    log.debug("start times : " + lastTx.getStartTs() + " " + tx.getStartTs());
    log.debug("sum1 : %,d  sum2 : %,d  diff : %,d\n", sum1, sum2, sum2 - sum1);

    File tmpFile = File.createTempFile("sb_dump", ".txt");
    Writer fw = new BufferedWriter(new FileWriter(tmpFile));

    Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());

    for (String cell : Iterables.transform(scanner, FluoFormatter::toString)) {
      fw.append(cell);
      fw.append("\n");
    }

    fw.close();

    log.debug("Dumped table : " + tmpFile);
  }

  private static HashMap<String, String> toMap(TestTransaction tx) throws Exception {
    HashMap<String, String> map = new HashMap<>();

    for (RowColumnValue rcv : tx.scanner().build()) {
      map.put(rcv.getRow().toString(), rcv.getValue().toString());
    }

    return map;
  }
}
