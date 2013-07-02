/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.accismus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.util.Stat;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * This test starts multiple thread that randomly transfer between accounts. At any given time the sum of all money in the bank should be the same, therefore
 * the average should not vary.
 */
public class StochasticBank {
  
  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  private static ZooKeeper zk;
  
  private static final Map<Column,Class<? extends Observer>> EMPTY_OBSERVERS = new HashMap<Column,Class<? extends Observer>>();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), secret);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
    
    zk = new ZooKeeper(cluster.getZooKeepers(), 30000, null);
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }

  private static AtomicInteger txCount = new AtomicInteger();

  @Test
  public void testConcurrency() throws Exception {
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    Operations.initialize(conn, "/test1", "bank", EMPTY_OBSERVERS);
    Configuration config = new Configuration(zk, "/test1", conn);
    
    conn.tableOperations().setProperty("bank", Property.TABLE_MAJC_RATIO.getKey(), "1");
    
    int numAccounts = 5000;
    
    AtomicBoolean runFlag = new AtomicBoolean(true);
    
    populate(config, numAccounts);
    
    List<Thread> threads = startTransfers(config, numAccounts, 20, runFlag);
    
    runVerifier(config, numAccounts, 100);
    
    runFlag.set(false);
    
    for (Thread thread : threads) {
      thread.join();
    }
    
    System.out.println("txCount : " + txCount.get());
    
    runVerifier(config, numAccounts, 1);
  }

  private static Column balanceCol = new Column("data", "balance");

  private static void populate(Configuration config, int numAccounts) throws Exception {
    Transaction tx = new Transaction(config);
    
    for(int i = 0; i < numAccounts; i++){
      tx.set(fmtAcct(i), balanceCol, "1000");
    }
    
    tx.commit();
  }
  
  private static String fmtAcct(int i) {
    return String.format("%09d", i);
  }

  private static List<Thread> startTransfers(final Configuration config, final int numAccounts, int numThreads, final AtomicBoolean runFlag) {
    
    ArrayList<Thread> threads = new ArrayList<Thread>();
    
    for (int i = 0; i < numThreads; i++) {
      Runnable task = new Runnable() {
        Random rand = new Random();
        
        public void run() {
          while (runFlag.get()) {
            int acct1 = rand.nextInt(numAccounts);
            int acct2 = rand.nextInt(numAccounts);
            while (acct1 == acct2)
              acct2 = rand.nextInt(numAccounts);
            int amt = rand.nextInt(100);
            
            transfer(config, fmtAcct(acct1), fmtAcct(acct2), amt);
          }
          
        }
        
      };
      Thread thread = new Thread(task);
      thread.start();
      threads.add(thread);
    }
    
    return threads;
  }
  
  private static void transfer(Configuration config, String from, String to, int amt) {
    try {
      
      boolean commited = false;
      
      while (!commited) {
        try {
          Transaction tx = new Transaction(config);
          int bal1 = Integer.parseInt(tx.get(from, balanceCol).toString());
          int bal2 = Integer.parseInt(tx.get(to, balanceCol).toString());
          
          if (bal1 - amt >= 0) {
            tx.set(from, balanceCol, (bal1 - amt) + "");
            tx.set(to, balanceCol, (bal2 + amt) + "");
          } else {
            break;
          }
          
          commited = tx.commit();
        } catch (StaleScanException sse) {
          // retry
        }
      }
      
      txCount.incrementAndGet();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void runVerifier(Configuration config, int numAccounts, int num) {
    Transaction lastTx = null;

    try {

      for (int i = 0; i < num; i++) {
        
        if (i == num / 2) {
          config.getConnector().tableOperations().compact(config.getTable(), null, null, true, false);
        }

        long t1 = System.currentTimeMillis();

        Transaction tx = new Transaction(config);
        RowIterator iter = tx.get(new ScannerConfiguration());
        
        Stat stat = new Stat();

        while (iter.hasNext()) {
          Entry<ByteSequence,ColumnIterator> colIter = iter.next();
          Entry<Column,ByteSequence> column = colIter.getValue().next();
          
          int amt = Integer.parseInt(column.getValue().toString());
          
          stat.addStat(amt);

        }
        
        long t2 = System.currentTimeMillis();

        System.out.printf("avg : %,9.2f  min : %,6d  max : %,6d  stddev : %1.2f  rate : %,6.2f\n", stat.getAverage(), stat.getMin(), stat.getMax(),
            stat.getStdDev(), numAccounts / ((t2 - t1) / 1000.0));

        if (stat.getSum() != numAccounts * 1000) {
          if (lastTx != null)
            printDiffs(lastTx, tx);

          return;
        }
        
        Assert.assertEquals(numAccounts * 1000, stat.getSum());

        lastTx = tx;
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private static void printDiffs(Transaction lastTx, Transaction tx) throws Exception {
    Map<String,String> bals1 = toMap(lastTx);
    Map<String,String> bals2 = toMap(tx);
    
    if (!bals1.keySet().equals(bals2.keySet())) {
      System.out.print("KS NOT EQ");
    }
    
    int sum1 = 0;
    int sum2 = 0;

    for (Entry<String,String> entry : bals1.entrySet()) {
      String val2 = bals2.get(entry.getKey());
      
      if (!entry.getValue().equals(val2)) {
        int v1 = Integer.parseInt(entry.getValue());
        int v2 = Integer.parseInt(val2);
        sum1 += v1;
        sum2 += v2;
        
        System.out.println(entry.getKey() + " " + entry.getValue() + " " + val2 + " " + (v2 - v1));
      }
    }

    System.out.printf("sum1 : %,d  sum2 : %,d  diff : %,d\n", sum1, sum2, sum2 - sum1);
  }
  
  private static HashMap<String,String> toMap(Transaction tx) throws Exception {
    HashMap<String,String> map = new HashMap<String,String>();
    
    RowIterator iter = tx.get(new ScannerConfiguration());
    while (iter.hasNext()) {
      Entry<ByteSequence,ColumnIterator> colIter = iter.next();
      Entry<Column,ByteSequence> column = colIter.getValue().next();
      
      map.put(colIter.getKey().toString(), column.getValue().toString());
    }
    
    return map;
  }
}
