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

import accismus.impl.thrift.OracleService;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.framework.recipes.leader.LeaderSelectorListener;
import com.netflix.curator.framework.recipes.leader.Participant;
import com.netflix.curator.framework.state.ConnectionState;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 
 */
public class OracleClient {
  
  private static final class TimeRequest {
    CountDownLatch cdl = new CountDownLatch(1);
    AtomicLong timestamp = new AtomicLong();
  }

  private class TimestampRetriever implements Runnable, LeaderSelectorListener {

    TimestampRetriever() throws Exception {

    }

    public void run() {
      ArrayList<TimeRequest> request = new ArrayList<TimeRequest>();
      
      try {
        
        OracleService.Client client = connect();

        while (true) {
          request.clear();
          request.add(queue.take());
          queue.drainTo(request);
          
          long start;

          while (true) {
            try {
              start = client.getTimestamps(config.getAccismusInstanceID(), request.size());
              break;
            } catch (TTransportException tte) {
              // TODO is this correct way to close?
              client.getInputProtocol().getTransport().close();
              client.getOutputProtocol().getTransport().close();
              
              // TODO maybe sleep a bit?
              client = connect();
            }
            
          }
          
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

    private CuratorFramework curatorFramework;

    private OracleService.Client connect() throws IOException, KeeperException, InterruptedException, TTransportException {
      // TODO use shared zookeeper or curator
      curatorFramework = CuratorFrameworkFactory.newClient(config.getConnector().getInstance().getZooKeepers(), new ExponentialBackoffRetry(1000, 300000));
      curatorFramework.start();

      LeaderSelector leaderSelector = new LeaderSelector(curatorFramework, config.getZookeeperRoot() + Constants.Zookeeper.ORACLE_SERVER, this);

      Participant participant = null;
      try {
          System.out.println(leaderSelector.getParticipants());
          participant = leaderSelector.getLeader();
      } catch (Exception e) {
          throw new RuntimeException("There was an error finding a leader in the list of Oracle servers.");
      }

      System.out.println("ID: "+ participant.getId());
      String host = participant.getId().split(":")[0];
      int port = Integer.parseInt(participant.getId().split(":")[1]);

      curatorFramework.close();

      TTransport transport = new TFastFramedTransport(new TSocket(host, port));
      transport.open();
      TProtocol protocol = new TCompactProtocol(transport);
      OracleService.Client client = new OracleService.Client(protocol);
      return client;
    }

      @Override
      public void takeLeadership(CuratorFramework curatorFramework) throws Exception {}

      @Override
      public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {}
  }

  private static Map<String,OracleClient> clients = new HashMap<String,OracleClient>();



  private Configuration config;
  private ArrayBlockingQueue<TimeRequest> queue = new ArrayBlockingQueue<TimeRequest>(1000);

  private OracleClient(Configuration config) throws Exception {
    this.config = config;
    
    // TODO make thread exit if idle for a bit, and start one when request arrives
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
    // this key differintiates between different instances of Accumulo and Accismus
    String key = config.getAccismusInstanceID();
    
    OracleClient client = clients.get(key);
    
    if (client == null) {
      try {
        client = new OracleClient(config);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      clients.put(key, client);
    }
    
    return client;
  }

}
