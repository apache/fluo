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
package io.fluo.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;

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


  @Test
  public void failoverTest() throws Exception {

    Thread.sleep(1000); // make sure first server becomes the leader
    OracleServer oserver2 = createOracle(9914);
    OracleServer oserver3 = createOracle(9915);

    oserver2.start();
    oserver3.start();

	  Thread.sleep(3000);

    CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(instance.getZooKeepers(), new ExponentialBackoffRetry(1000, 10));
    curatorFramework.start();

    OracleClient client = OracleClient.getInstance(config);

    long timestamp;
    for(long i = 0; i <= 5; i++) {
      timestamp = client.getTimestamp();
      assertEquals(i, timestamp);
    }

    oserver.stop();

    Thread.sleep(1000);
    assertEquals(1000, client.getTimestamp());

    oserver2.stop();

    Thread.sleep(1000);
    assertEquals(2000, client.getTimestamp());

    oserver3.stop();
  }
}
