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
package org.apache.accumulo.accismus.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.accumulo.accismus.Configuration;

/**
 * 
 */
public class OracleClient {
  
  private static final class TimeRequest {
    CountDownLatch cdl = new CountDownLatch(1);
    AtomicLong timestamp = new AtomicLong();
  }

  private class TimestampRetriever implements Runnable {
    private OracleServer server = new OracleServer();

    public void run() {
      ArrayList<TimeRequest> request = new ArrayList<TimeRequest>();
      
      try {
        while (true) {
          request.clear();
          request.add(queue.take());
          queue.drainTo(request);
          
          // TODO use thrift
          long start = server.getTimestamps(request.size());
          
          for (int i = 0; i < request.size(); i++) {
            TimeRequest tr = request.get(i);
            tr.timestamp.set(start + i);
            tr.cdl.countDown();
          }

        }
      } catch (Exception e) {
        // TODO
        e.printStackTrace();
      }
    }
  }

  private static Map<String,OracleClient> clients = new HashMap<String,OracleClient>();


  private Configuration config;
  private ArrayBlockingQueue<TimeRequest> queue = new ArrayBlockingQueue<TimeRequest>(1000);

  private OracleClient(Configuration config) {
    this.config = config;
    
    Thread thread = new Thread(new TimestampRetriever());
    thread.setDaemon(true);
    thread.start();
  }
  
  public long getTimestamp() throws Exception {
    TimeRequest tr = new TimeRequest();
    queue.add(tr);
    tr.cdl.await();
    return tr.timestamp.get();
  }

  public static synchronized OracleClient getInstance(Configuration config) {
    // TODO need a better key
    String key = config.getZookeeperRoot() + ":" + config.getAccumuloInstance() + ":" + config.getTable();
    
    OracleClient client = clients.get(key);
    
    if (client == null) {
      client = new OracleClient(config);
      clients.put(key, client);
    }
    
    return client;
  }

}
