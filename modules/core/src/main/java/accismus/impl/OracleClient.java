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

import accismus.core.util.UtilWaitThread;
import accismus.impl.support.CuratorCnxnListener;
import accismus.impl.thrift.OracleService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Connects to an oracle to retrieve timestamps. If mutliple oracle servers are run, it will automatically
 * fail over to different leaders.
 */
public class OracleClient {

  public static final Logger log = LoggerFactory.getLogger(OracleClient.class);

  private Participant currentLeader;

  private static final class TimeRequest {
    CountDownLatch cdl = new CountDownLatch(1);
    AtomicLong timestamp = new AtomicLong();
  }

  private class TimestampRetriever extends LeaderSelectorListenerAdapter implements Runnable, PathChildrenCacheListener {

    private LeaderSelector leaderSelector;
    private CuratorFramework curatorFramework;
    private OracleService.Client client;
    private PathChildrenCache pathChildrenCache;

    private TTransport transport;

    public void run() {

      String zkPath = config.getZookeeperRoot() + Constants.Zookeeper.ORACLE_SERVER;

      try {
        curatorFramework = CuratorFrameworkFactory.newClient(config.getConnector().getInstance().getZooKeepers(), new ExponentialBackoffRetry(1000, 10));
        CuratorCnxnListener cnxnListener = new CuratorCnxnListener();
        curatorFramework.getConnectionStateListenable().addListener(cnxnListener);
        curatorFramework.start();

        while (!cnxnListener.isConnected())
          Thread.sleep(200);

        pathChildrenCache = new PathChildrenCache(curatorFramework, zkPath, true);
        pathChildrenCache.getListenable().addListener(this);
        pathChildrenCache.start();

        leaderSelector = new LeaderSelector(curatorFramework, zkPath, this);

        connect();
        doWork();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    /**
     * It's possible an Oracle has gone into a bad state. Upon the leader being changed, we want to update our state
     */
    @Override
    public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {

      if (pathChildrenCacheEvent.getType().toString().startsWith("CHILD")) {
        Participant participant = leaderSelector.getLeader();
        synchronized (this) {
          if(isLeader(participant))
            currentLeader = leaderSelector.getLeader();
          else
            currentLeader = null;
        }
      }
    }

    private void doWork() {

      ArrayList<TimeRequest> request = new ArrayList<TimeRequest>();

      while (true) {

        try {
          request.clear();
          request.add(queue.take());
          queue.drainTo(request);

          long start;

          while (true) {

            try {
              String currentLeaderId;
              OracleService.Client localClient;
              synchronized (this) {
                currentLeaderId = getOracle();
                localClient = client;
              }

              start = localClient.getTimestamps(config.getAccismusInstanceID(), request.size());

              String leaderId = getOracle();
              if(leaderId != null && !leaderId.equals(currentLeaderId)) {
                reconnect();
                continue;
              }

              break;

            } catch (TTransportException tte) {
              log.info("Oracle connection lost. Retrying...");
              reconnect();
            } catch (TException e) {
              e.printStackTrace();
            }
          }

          for (int i = 0; i < request.size(); i++) {
            TimeRequest tr = request.get(i);
            tr.timestamp.set(start + i);
            tr.cdl.countDown();
          }

        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }

    private synchronized void connect() throws IOException, KeeperException, InterruptedException, TTransportException {

      getLeader();
      while (true) {
        log.debug("Connecting to oracle at " + currentLeader.getId());
        String[] hostAndPort = currentLeader.getId().split(":");

        String host = hostAndPort[0];
        int port = Integer.parseInt(hostAndPort[1]);

        try {
          transport = new TFastFramedTransport(new TSocket(host, port));
          transport.open();
          TProtocol protocol = new TCompactProtocol(transport);
          client = new OracleService.Client(protocol);
          log.info("Connected to oracle at " + getOracle());
          break;
        } catch (TTransportException e) {
          sleepRandom();
          getLeader();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    /**
     * Atomically closes current connection and connects to the current leader
     */
    private synchronized void reconnect() throws InterruptedException, TTransportException, KeeperException, IOException {
      close();
      connect();
    }

    private void close() {
      if(transport.isOpen())
        transport.close();
    }

    private boolean getLeaderAttempt() {
      Participant possibleLeader = null;
      try {
        possibleLeader = leaderSelector.getLeader();
      } catch (KeeperException e) {
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      if (isLeader(possibleLeader)) {
        currentLeader = possibleLeader;
        return true;
      }
      return false;
    }

    /**
     * Attempt to retrieve a leader until one is found
     */
    private void getLeader() {
      boolean found = getLeaderAttempt();
      while (!found) {
        sleepRandom();
        found = getLeaderAttempt();
      }
    }

    /**
     * Sleep a random amount of time from 100ms to 1sec
     */
    private void sleepRandom() {
      UtilWaitThread.sleep(100 + (long) (1000 * Math.random()));
    }

    private boolean isLeader(Participant participant) {
      return participant != null && participant.isLeader();
    }


    /**
     * NOTE: This isn't competing for leadership, so it doesn't need to be started.
     */
    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
    }
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

  /**
   * Return the oracle that the current client is connected to.
   */
  public synchronized String getOracle() {
    return currentLeader != null ? currentLeader.getId() : null;
  }

  /**
   * Create an instance of an OracleClient and cache it by the Accismus instance id`
   *
   * @param config
   * @return
   */
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
