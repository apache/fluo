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
package accismus.impl;

import org.junit.Test;

import accismus.core.util.PortUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;

/**
 *
 */
public class OracleIT extends Base {

  @Test
  public void testRestart() throws Exception {
    OracleClient client = OracleClient.getInstance(config);

    long ts1 = client.getTimestamp();
    long ts2 = client.getTimestamp();

    oserver.stop();
    oserver.start();

    long ts3 = client.getTimestamp();
    long ts4 = client.getTimestamp();

    assertTrue(ts1 + " " + ts2, ts1 < ts2);
    assertTrue(ts2 + " " + ts3, ts2 < ts3);
    assertTrue(ts3 + " " + ts4, ts3 < ts4);
  }

  private static class TimestampFetcher implements Runnable {
    private int numToGet;
    private Configuration config;
    private List<Long> output;
    private CountDownLatch cdl;

    TimestampFetcher(int numToGet, Configuration config, List<Long> output, CountDownLatch cdl) {
      this.numToGet = numToGet;
      this.config = config;
      this.output = output;
      this.cdl = cdl;
    }

    @Override
    public void run() {
      OracleClient oclient = OracleClient.getInstance(config);

      for (int i = 0; i < numToGet; i++) {
        try {
          output.add(oclient.getTimestamp());
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      cdl.countDown();
    }
  }

  @Test
  public void threadTest() throws Exception {

    int numThreads = 20;
    int numTimes = 100;

    List<Long> output = Collections.synchronizedList(new ArrayList<Long>());
    ExecutorService tpool = Executors.newFixedThreadPool(numThreads);
    CountDownLatch cdl = new CountDownLatch(numThreads);

    for (int i = 0; i < numThreads; i++) {
      tpool.execute(new TimestampFetcher(numTimes, config, output, cdl));
    }

    cdl.await();

    TreeSet<Long> ts1 = new TreeSet<Long>(output);

    assertEquals(numThreads * numTimes, ts1.size());

    cdl = new CountDownLatch(numThreads);
    output.clear();

    for (int i = 0; i < numThreads; i++) {
      tpool.execute(new TimestampFetcher(numTimes, config, output, cdl));
    }

    cdl.await();

    TreeSet<Long> ts2 = new TreeSet<Long>(output);

    assertEquals(numThreads * numTimes, ts2.size());
    assertTrue(ts1.last() < ts2.first());

    tpool.shutdown();
  }

  /**
   * If multiple {@link OracleServer} instances are competing leadership and fail, the {@link OracleClient} should
   * failover to them as they go down and serve up new blocks of timestamps.
   */
  @Test
  public void failover_newTimestampRequested() throws Exception {

    while (!oserver.isConnected())
      Thread.sleep(100);

    int port2 = PortUtils.getRandomFreePort();
    int port3 = PortUtils.getRandomFreePort();
    
    OracleServer oserver2 = createExtraOracle(port2);
    OracleServer oserver3 = createExtraOracle(port3);

    oserver2.start();
    while (!oserver2.isConnected())
      Thread.sleep(100);

    oserver3.start();
    while (!oserver3.isConnected())
      Thread.sleep(100);

    OracleClient client = OracleClient.getInstance(config);

    long timestamp;
    for (long i = 2; i <= 7; i++) {
      timestamp = client.getTimestamp();
      assertEquals(i, timestamp);
    }

    assertTrue(client.getOracle().endsWith(Integer.toString(config.getOraclePort())));

    oserver.stop();

    Thread.sleep(1000);
    assertEquals(1002, client.getTimestamp());
    assertTrue(client.getOracle().endsWith(Integer.toString(port2)));

    oserver2.stop();

    Thread.sleep(2000);
    assertEquals(2002, client.getTimestamp());
    assertTrue(client.getOracle().endsWith(Integer.toString(port3)));

    oserver3.stop();
  }

  /**
   * If an {@link OracleServer} goes away and comes back, the client should automatically reconnect
   * and start a new block of timestamps (making sure that no timestamp should ever go backwards).
   */
  @Test
  public void singleOracle_goesAwayAndComesBack() throws Exception {

    while (!oserver.isConnected())
      Thread.sleep(100);

    OracleClient client = OracleClient.getInstance(config);

    long timestamp;
    for (long i = 2; i <= 7; i++) {
      timestamp = client.getTimestamp();
      assertEquals(i, timestamp);
    }

    oserver.stop();

    Thread.sleep(2000);

    assertNull(client.getOracle());

    oserver.start();

    while (!oserver.isConnected())
      Thread.sleep(100);

    assertEquals(1002, client.getTimestamp());

    assertTrue(client.getOracle().endsWith(Integer.toString(config.getOraclePort())));

    oserver.stop();
  }


  @Test
  public void threadFailoverTest() throws Exception {

    int numThreads = 20;
    int numTimes = 100;

    List<Long> output = Collections.synchronizedList(new ArrayList<Long>());
    ExecutorService tpool = Executors.newFixedThreadPool(numThreads);
    CountDownLatch cdl = new CountDownLatch(numThreads);

    
    int port2 = PortUtils.getRandomFreePort();
    int port3 = PortUtils.getRandomFreePort();
    
    OracleServer oserver2 = createExtraOracle(port2);

    oserver2.start();
    while (!oserver2.isConnected())
      Thread.sleep(100);

    OracleServer oserver3 = createExtraOracle(port3);

    oserver3.start();
    while (!oserver3.isConnected())
      Thread.sleep(100);

    for (int i = 0; i < numThreads; i++) {
      tpool.execute(new TimestampFetcher(numTimes, config, output, cdl));

      if(i == 10)
        oserver.stop();
    }

    cdl.await();

    TreeSet<Long> ts1 = new TreeSet<Long>(output);

    assertEquals(numThreads * numTimes, ts1.size());

    cdl = new CountDownLatch(numThreads);
    output.clear();

    for (int i = 0; i < numThreads; i++) {
      tpool.execute(new TimestampFetcher(numTimes, config, output, cdl));

      if(i == 5)
        oserver2.stop();
    }

    cdl.await();

    TreeSet<Long> ts2 = new TreeSet<Long>(output);

    assertEquals(numThreads * numTimes, ts2.size());
    assertTrue(ts1.last() < ts2.first());

    tpool.shutdown();
    oserver3.stop();
  }
}
